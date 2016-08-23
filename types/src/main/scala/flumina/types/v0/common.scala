package flumina.types.v0

import scodec.bits.{BitVector, crc}
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}
import flumina.types._

final case class Message(magicByte: Int, attributes: Int, key: Vector[Byte], value: Vector[Byte])

object Message {

  implicit val codec: Codec[Message] = new Codec[Message] {
    override def encode(value: Message): Attempt[BitVector] = for {
      magicByte <- int8.encode(value.magicByte)
      attributes <- int8.encode(value.attributes)
      key <- kafkaBytes.encode(value.key)
      value <- kafkaBytes.encode(value.value)
    } yield {
      val payload = magicByte ++ attributes ++ key ++ value
      crc.crc32(payload) ++ payload
    }

    override def sizeBound: SizeBound = SizeBound.unknown

    override def decode(bits: BitVector): Attempt[DecodeResult[Message]] = {
      for {
        crcPayload <- fixedSizeBits(32, scodec.codecs.bits).decode(bits)

        _ <- if (crcPayload.value == crc.crc32(crcPayload.remainder)) Attempt.successful(())
        else Attempt.failure(Err(s"Payload checksum: ${crcPayload.value} is not equal to the calculated checksum"))

        magicByte <- int8.decode(crcPayload.remainder)
        attributes <- int8.decode(magicByte.remainder)
        key <- kafkaBytes.decode(attributes.remainder)
        value <- kafkaBytes.decode(key.remainder)
      } yield {
        DecodeResult(Message(magicByte.value, attributes.value, key.value, value.value), value.remainder)
      }
    }
  }
}

final case class MessageSetEntry(offset: Long, message: Message)

object MessageSetEntry {
  implicit def messageSet(implicit message: Codec[Message]): Codec[MessageSetEntry] =
    (("offset" | int64) :: ("message" | variableSizeBytes(int32, message))).as[MessageSetEntry]
}