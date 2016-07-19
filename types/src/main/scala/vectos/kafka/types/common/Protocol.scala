package vectos.kafka.types.common

import scodec._
import scodec.codecs._

import vectos.kafka.types._

final case class ConsumerProtocolMetadata(version: Int, subscriptions: Vector[Option[String]], userData: Vector[Byte])

object ConsumerProtocolMetadata {
  implicit val codec: Codec[ConsumerProtocolMetadata] =
    (("version" | int16) :: ("subscriptions" | kafkaArray(kafkaString)) :: ("userData" | kafkaBytes)).as[ConsumerProtocolMetadata]
}