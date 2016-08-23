package flumina.types.v0

import scodec._
import scodec.codecs._
import flumina.types._

final case class SyncGroupGroupAssignmentRequest(memberId: Option[String], assignmentData: Vector[Byte])

object SyncGroupGroupAssignmentRequest {
  implicit def codec: Codec[SyncGroupGroupAssignmentRequest] =
    (("memberId" | kafkaString) :: ("assignmentData" | kafkaBytes)).as[SyncGroupGroupAssignmentRequest]
}