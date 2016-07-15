package vectos.kafka.types.v0.messages

import scodec._
import scodec.codecs._

final case class JoinGroupProtocolRequest(protocolName: Option[String], metadata: Vector[Byte])
final case class JoinGroupMemberResponse(memberId: Option[String], metadata: Vector[Byte])

object JoinGroupProtocolRequest {
  implicit def codec: Codec[JoinGroupProtocolRequest] =
    (("protocolName" | kafkaString) :: ("metadata" | kafkaBytes)).as[JoinGroupProtocolRequest]
}

object JoinGroupMemberResponse {
  implicit def codec: Codec[JoinGroupMemberResponse] =
    (("memberId" | kafkaString) :: ("metadata" | kafkaBytes)).as[JoinGroupMemberResponse]
}
