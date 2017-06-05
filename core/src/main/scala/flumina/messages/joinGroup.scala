package flumina.messages

import scodec._
import scodec.codecs._
import flumina._
import scodec.bits.ByteVector

final case class JoinGroupProtocolRequest(protocolName: String, metadata: ByteVector)
final case class JoinGroupMemberResponse(memberId: String, metadata: ByteVector)

object JoinGroupProtocolRequest {
  val codec: Codec[JoinGroupProtocolRequest] =
    (("protocolName" | kafkaRequiredString) :: ("metadata" | kafkaBytes)).as[JoinGroupProtocolRequest]
}

object JoinGroupMemberResponse {
  val codec: Codec[JoinGroupMemberResponse] =
    (("memberId" | kafkaRequiredString) :: ("metadata" | kafkaBytes)).as[JoinGroupMemberResponse]
}
