package flumina

import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, SizeBound}

private[flumina] class KafkaStringCodec extends Codec[Option[String]] {
  val codec = variableSizeBytes(int16, ascii)

  override def decode(bits: BitVector): Attempt[DecodeResult[Option[String]]] =
    for {
      size <- int16.decode(bits)
      str <- if (size.value == -1) Attempt.successful(DecodeResult(None, size.remainder))
      else
        variableSizeBytes(provide(size.value), ascii).decode(size.remainder).map(_.map(Some.apply))
    } yield str
  override def encode(value: Option[String]): Attempt[BitVector] = value match {
    case Some(str) => codec.encode(str)
    case None      => int16.encode(-1)
  }

  override def sizeBound: SizeBound = codec.sizeBound
}

private[flumina] class KafkaBytesCodec extends Codec[ByteVector] {
  val codec = variableSizeBytes(int32, bytes)

  override def decode(bits: BitVector): Attempt[DecodeResult[ByteVector]] =
    for {
      size <- int32.decode(bits)
      xs <- if (size.value == -1)
        Attempt.successful(DecodeResult(ByteVector.empty, size.remainder))
      else codec.decode(bits)
    } yield xs

  override def encode(value: ByteVector): Attempt[BitVector] =
    if (value.isEmpty) int32.encode(-1)
    else codec.encode(value)

  override def sizeBound: SizeBound = codec.sizeBound
}

private[flumina] class KafkaNullableArrayCodec[A](valueCodec: Codec[A]) extends Codec[Vector[A]] {
  val codec = vectorOfN(int32, valueCodec)

  override def decode(bits: BitVector): Attempt[DecodeResult[Vector[A]]] =
    for {
      size <- int32.decode(bits)
      xs <- if (size.value == -1) Attempt.successful(DecodeResult(Vector.empty[A], size.remainder))
      else codec.decode(bits)
    } yield xs

  override def encode(value: Vector[A]): Attempt[BitVector] =
    if (value.isEmpty) int32.encode(-1)
    else codec.encode(value)

  override def sizeBound: SizeBound = codec.sizeBound
}
