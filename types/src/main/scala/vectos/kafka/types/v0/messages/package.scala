package vectos.kafka.types.v0

import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult, SizeBound}
import scodec.codecs._

package object messages {

  val kafkaString: Codec[Option[String]] = new KafkaStringCodec
  val kafkaBytes: Codec[Vector[Byte]] = new KafkaBytes

  def kafkaArray[A](valueCodec: Codec[A]): Codec[Vector[A]] = vectorOfN(int32, valueCodec)
}
