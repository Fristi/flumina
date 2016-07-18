package vectos.kafka.types.v0

import scodec._
import scodec.codecs._
import vectos.kafka.types._

final case class DescribeGroupsGroupMemberResponse(
  memberId:         Option[String],
  clientId:         Option[String],
  clientHost:       Option[String],
  memberMetadata:   Vector[Byte],
  memberAssignment: Vector[Byte]
)

final case class DescribeGroupsGroupResponse(
  errorCode:    KafkaResult,
  groupId:      Option[String],
  state:        Option[String],
  protocolType: Option[String],
  protocol:     Option[String],
  members:      Vector[DescribeGroupsGroupMemberResponse]
)

object DescribeGroupsGroupMemberResponse {
  implicit def codec: Codec[DescribeGroupsGroupMemberResponse] = (
    ("memberId" | kafkaString) ::
    ("clientId" | kafkaString) ::
    ("clientHost" | kafkaString) ::
    ("memberMetadata" | kafkaBytes) ::
    ("memberAssignment" | kafkaBytes)
  ).as[DescribeGroupsGroupMemberResponse]
}

object DescribeGroupsGroupResponse {
  implicit def codec(implicit kafkaResult: Codec[KafkaResult], member: Codec[DescribeGroupsGroupMemberResponse]): Codec[DescribeGroupsGroupResponse] = (
    ("kafkaResult" | kafkaResult) ::
    ("groupId" | kafkaString) ::
    ("state" | kafkaString) ::
    ("protocolType" | kafkaString) ::
    ("protocol" | kafkaString) ::
    ("members" | kafkaArray(member))
  ).as[DescribeGroupsGroupResponse]

}