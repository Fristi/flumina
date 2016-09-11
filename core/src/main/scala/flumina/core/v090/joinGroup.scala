package flumina.core.v090

import scodec._
import scodec.codecs._
import flumina.core._
import scodec.bits.ByteVector

final case class JoinGroupProtocolRequest(protocolName: String, metadata: ByteVector)
final case class JoinGroupMemberResponse(memberId: String, metadata: ByteVector)

object JoinGroupProtocolRequest {
  implicit def codec: Codec[JoinGroupProtocolRequest] =
    (("protocolName" | kafkaRequiredString) :: ("metadata" | kafkaBytes)).as[JoinGroupProtocolRequest]
}

object JoinGroupMemberResponse {
  implicit def codec: Codec[JoinGroupMemberResponse] =
    (("memberId" | kafkaRequiredString) :: ("metadata" | kafkaBytes)).as[JoinGroupMemberResponse]
}
