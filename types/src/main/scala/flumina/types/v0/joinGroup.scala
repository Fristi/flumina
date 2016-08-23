package flumina.types.v0

import scodec._
import scodec.codecs._
import flumina.types._

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
