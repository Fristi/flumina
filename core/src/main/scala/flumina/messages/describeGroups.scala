package flumina.messages

import flumina._
import scodec._
import scodec.codecs._
import scodec.bits.ByteVector

final case class DescribeGroupsGroupMemberResponse(
  memberId:         String,
  clientId:         String,
  clientHost:       String,
  memberMetadata:   ByteVector,
  memberAssignment: ByteVector
)

final case class DescribeGroupsGroupResponse(
  kafkaResult:  KafkaResult,
  groupId:      String,
  state:        String,
  protocolType: String,
  protocol:     String,
  members:      Vector[DescribeGroupsGroupMemberResponse]
)

object DescribeGroupsGroupMemberResponse {
  val codec: Codec[DescribeGroupsGroupMemberResponse] = (
    ("memberId" | kafkaRequiredString) ::
    ("clientId" | kafkaRequiredString) ::
    ("clientHost" | kafkaRequiredString) ::
    ("memberMetadata" | kafkaBytes) ::
    ("memberAssignment" | kafkaBytes)
  ).as[DescribeGroupsGroupMemberResponse]
}

object DescribeGroupsGroupResponse {
  val codec: Codec[DescribeGroupsGroupResponse] = (
    ("kafkaResult" | KafkaResult.codec) ::
    ("groupId" | kafkaRequiredString) ::
    ("state" | kafkaRequiredString) ::
    ("protocolType" | kafkaRequiredString) ::
    ("protocol" | kafkaRequiredString) ::
    ("members" | kafkaArray(DescribeGroupsGroupMemberResponse.codec))
  ).as[DescribeGroupsGroupResponse]

}