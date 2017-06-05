package flumina.messages

import scodec._
import scodec.codecs._
import flumina._
import scodec.bits.ByteVector

final case class SyncGroupGroupAssignmentRequest(memberId: String, assignmentData: ByteVector)

object SyncGroupGroupAssignmentRequest {
  val codec: Codec[SyncGroupGroupAssignmentRequest] =
    (("memberId" | kafkaRequiredString) :: ("assignmentData" | kafkaBytes))
      .as[SyncGroupGroupAssignmentRequest]
}
