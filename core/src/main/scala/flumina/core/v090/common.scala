package flumina.core.v090

import java.util.Date

import flumina.core._
import flumina.core.ir.{Compression, MessageVersion}
import scodec.bits.{BitVector, ByteVector, crc}
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, Encoder, Err, SizeBound}
import shapeless._

import scala.annotation.tailrec
import scala.language.postfixOps

sealed trait TimeData {
  def time: Date
}

object TimeData {
  final case class LogAppendTime(time: Date) extends TimeData
  final case class CreateTime(time: Date) extends TimeData
}

object Message {

  final case class SingleMessage(
    offset:    Long,
    version:   MessageVersion,
    timeStamp: Option[TimeData],
    key:       ByteVector,
    value:     ByteVector
  ) extends Message

  final case class CompressedMessages(
    offset:      Long,
    version:     MessageVersion,
    compression: Compression,
    timeStamp:   Option[TimeData],
    messages:    Vector[Message]
  ) extends Message

}

sealed trait Message { self =>

  def offset: Long

  def updateOffset(newOffset: Long): Message = self match {
    case sm: Message.SingleMessage      => sm.copy(offset = newOffset)
    case cm: Message.CompressedMessages => cm.copy(offset = newOffset)
  }

}

object MessageSetCodec {

  private class KafkaPartialVectorCodec[A](valueCodec: Codec[A]) extends Codec[Vector[A]] {

    override def encode(value: Vector[A]) = Encoder.encodeSeq(valueCodec)(value)

    override def decode(bits: BitVector): Attempt[DecodeResult[Vector[A]]] = {
      @tailrec
      def extract(acc: List[A], bitVector: BitVector): Attempt[DecodeResult[Vector[A]]] = {
        valueCodec.decode(bitVector) match {
          case Attempt.Successful(DecodeResult(value, remainder)) =>
            extract(value :: acc, remainder)
          case Attempt.Failure(err) =>
            Attempt.successful(DecodeResult(acc.reverse.toVector, BitVector.empty))
        }
      }

      extract(List.empty, bits)
    }

    override def sizeBound: SizeBound = SizeBound.unknown
  }

  val messageSetCodec: Codec[Vector[Message]] = {
    val entry: Codec[Message] =
      "Message Entry" | (
        ("Offset" | int64) ~
        variableSizeBytes("Message Size" | int32, impl.messageCodec)
      ).xmap(
          { case (offset, msg) => msg.updateOffset(offset) }, msg => (msg.offset, msg)
        )

    new KafkaPartialVectorCodec(entry)
  }

  object impl {
    val MagicByteV0: Byte = 0
    val MagicByteV1: Byte = 1

    //  3 bits holding information whether the message is or is not compressed
    //  yielding to None indicates message is not compressed
    val compressionAttribute: Codec[Option[Compression]] = {
      def decode(compression: Int): Attempt[Option[Compression]] = {
        if (compression === 0) Attempt.successful(None)
        else Compression(compression).map(Some(_))
      }
      def encode(compression: Option[Compression]): Attempt[Int] =
        Attempt.successful(compression.map(_.id).getOrElse(0))
      int(3).exmap(decode, encode)
    }

    val timeCodec: Codec[Option[Date]] = {
      int64.xmap(
        { t => if (t < 0) None else Some(new Date(t)) }, {
          _.map(_.getTime).getOrElse(-1l)
        }
      )
    }

    def decodeMessage(
      version: MessageVersion, compression: Option[Compression], timeFlag: Boolean, time: Option[Date], k: ByteVector, v: ByteVector
    ): Attempt[Message] = {
      val timeData: Option[TimeData] = time.map {
        t => if (timeFlag) TimeData.LogAppendTime(t) else TimeData.CreateTime(t)
      }

      def decodeCompressed(compression: Compression)(content: ByteVector): Attempt[Message] = {
        messageSetCodec.decode(content.bits).flatMap { result =>
          if (result.remainder.nonEmpty) Attempt.failure(Err(s"Nonepmty remainder when decoding compressed messgaes : ${result.remainder}"))
          else Attempt.successful(Message.CompressedMessages(0, version, compression, timeData, result.value))
        }
      }

      compression match {
        case None => Attempt.successful(Message.SingleMessage(0, version, timeData, k, v))

        case Some(compressionType) =>
          compressionType match {
            case Compression.GZIP   => GZipCompression.inflate(v) flatMap decodeCompressed(Compression.GZIP)
            case Compression.Snappy => SnappyCompression.inflate(v) flatMap decodeCompressed(Compression.Snappy)
            case _                  => sys.error("Impossible?")
          }
      }
    }

    private def encodeMessage(msg: Message) = {

      def mkTime(timeStamp: Option[TimeData]): (Boolean, Option[Date]) = {
        val timeFlag = timeStamp.exists {
          case _: TimeData.LogAppendTime => true
          case _: TimeData.CreateTime    => false
        }
        val time = timeStamp.map(_.time)
        timeFlag -> time
      }

      msg match {
        case sm: Message.SingleMessage => attempt {
          val (timeFlag, time) = mkTime(sm.timeStamp)
          () :: timeFlag :: None :: time :: sm.key :: sm.value :: HNil
        }

        case cm: Message.CompressedMessages =>
          def encodeCompressed(messages: Vector[Message]): Attempt[ByteVector] =
            messageSetCodec.encode(messages).map(_.bytes)

          val value =
            cm.compression match {
              case Compression.GZIP   => encodeCompressed(cm.messages).flatMap(GZipCompression.deflate)
              case Compression.Snappy => encodeCompressed(cm.messages).flatMap(SnappyCompression.deflate)
              case _                  => sys.error("Impossible?")
            }

          val (timeFlag, time) = mkTime(cm.timeStamp)

          value.map { vb =>
            () :: timeFlag :: Some(cm.compression) :: time :: ByteVector.empty :: vb :: HNil
          }

      }
    }

    val messageCodec: Codec[Message] = {
      "Message" | crcChecksum(
        ("MagicByte" | byte).flatZip {
          case MagicByteV0 => v0Codec
          case MagicByteV1 => v1Codec
          case other       => fail[Message](Err(s"Unexpected message magic: $other"))
        }.xmap(_._2, m => magicOf(m) -> m)
      )
    }

    def v0Codec: Codec[Message] = {

      "V0" | (
        ("Ignored Attribute" | ignore(5)) ::
        ("Compression" | compressionAttribute) ::
        ("Key" | variableSizeBytes(int32, bytes)) ::
        ("Value" | variableSizeBytes(int32, bytes))
      ).exmap(
          {
            case _ :: compression :: k :: v :: HNil =>
              decodeMessage(MessageVersion.V0, compression, false, None, k, v)
          }, (encodeMessage _).andThen(_.map {
            case _ :: timeFlag :: compression :: time :: k :: v :: HNil =>
              () :: compression :: k :: v :: HNil
          })
        )

    }

    def v1Codec: Codec[Message] = {

      "V1" | (
        ("Ignored Attribute" | ignore(4)) ::
        ("Time flag" | kafkaBool) ::
        ("Compression" | compressionAttribute) ::
        ("Time" | timeCodec) ::
        ("Key" | variableSizeBytes(int32, bytes)) ::
        ("Value" | variableSizeBytes(int32, bytes))
      ).exmap(
          {
            case _ :: timeFlag :: compression :: time :: k :: v :: HNil =>
              decodeMessage(MessageVersion.V1, compression, timeFlag, time, k, v)
          }, encodeMessage
        )

    }

    /**
     * codec that will write crc checksum before the encoded data by codec and when decoding
     * it verifies the crc will match before decoding reminder
     */
    def crcChecksum[A](codec: Codec[A]): Codec[A] = {
      def decode(crc32: BitVector, data: BitVector) =
        if (crc.crc32(data) =/= crc32) Attempt.failure(Err("CRC of message does not match"))
        else codec.decodeValue(data)

      def encode(a: A) = codec.encode(a).map(bv => crc.crc32(bv) -> bv)

      (("CRC32" | bits(32)) ~ ("Data" | bits)).exmap(decode _ tupled, encode)

    }

    def magicOf(m: Message): Byte = {
      m match {
        case sm: Message.SingleMessage      => sm.version.id.toByte
        case cm: Message.CompressedMessages => cm.version.id.toByte

      }
    }

  }

}