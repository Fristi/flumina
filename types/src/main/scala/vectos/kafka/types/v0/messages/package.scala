package vectos.kafka.types.v0

import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, SizeBound}
import scodec.codecs._

package object messages {

  private class KafkaStringCodec extends Codec[Option[String]] {
    val codec = variableSizeBytes(int16, ascii)

    override def decode(bits: BitVector): Attempt[DecodeResult[Option[String]]] = for {
      size <- int16.decode(bits)
      str <- if(size.value == -1) Attempt.successful(DecodeResult(None, size.remainder))
            else variableSizeBytes(provide(size.value), ascii).decode(size.remainder).map(_.map(Some.apply))
    } yield str
    override def encode(value: Option[String]): Attempt[BitVector] = value match {
      case Some(str) => codec.encode(str)
      case None => Attempt.successful(BitVector(-1))
    }

    override def sizeBound: SizeBound = codec.sizeBound
  }

  private class KafkaArrayCodec[A](valueCodec: Codec[A]) extends Codec[Vector[A]] {
    val codec = vectorOfN(int32, valueCodec)

    override def decode(bits: BitVector): Attempt[DecodeResult[Vector[A]]] = for {
      size <- int32.decode(bits)
      xs <- if(size.value == -1) Attempt.successful(DecodeResult(Vector.empty[A], size.remainder))
            else vectorOfN(provide(size.value), valueCodec).decode(size.remainder)
    } yield xs

    override def encode(value: Vector[A]): Attempt[BitVector] =
      if(value.isEmpty) Attempt.successful(BitVector(-1))
      else codec.encode(value)

    override def sizeBound: SizeBound = codec.sizeBound
  }

  val kafkaString: Codec[Option[String]] = new KafkaStringCodec
  def kafkaArray[A](valueCodec: Codec[A]): Codec[Vector[A]] = new KafkaArrayCodec(valueCodec)
}