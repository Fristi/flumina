package flumina.core.v090

import scodec._
import scodec.codecs._
import flumina.core._
import scodec.bits.ByteVector

final case class SyncGroupGroupAssignmentRequest(memberId: String, assignmentData: ByteVector)

object SyncGroupGroupAssignmentRequest {
  implicit def codec: Codec[SyncGroupGroupAssignmentRequest] =
    (("memberId" | kafkaRequiredString) :: ("assignmentData" | kafkaBytes)).as[SyncGroupGroupAssignmentRequest]
}