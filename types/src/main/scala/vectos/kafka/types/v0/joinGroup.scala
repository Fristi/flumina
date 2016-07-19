package vectos.kafka.types.v0

import scodec._
import scodec.codecs._
import vectos.kafka.types._
import vectos.kafka.types.common.ConsumerProtocolMetadata

final case class JoinGroupProtocolRequest(protocolName: Option[String], metadata: Vector[ConsumerProtocolMetadata])
final case class JoinGroupMemberResponse(memberId: Option[String], metadata: Vector[Byte])

object JoinGroupProtocolRequest {
  implicit def codec(implicit protocolMetadata: Codec[ConsumerProtocolMetadata]): Codec[JoinGroupProtocolRequest] =
    (("protocolName" | kafkaString) :: ("metadata" | kafkaArray(protocolMetadata))).as[JoinGroupProtocolRequest]
}

object JoinGroupMemberResponse {
  implicit def codec: Codec[JoinGroupMemberResponse] =
    (("memberId" | kafkaString) :: ("metadata" | kafkaBytes)).as[JoinGroupMemberResponse]
}
