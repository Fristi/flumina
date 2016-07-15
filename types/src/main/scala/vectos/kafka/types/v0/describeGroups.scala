package vectos.kafka.types.v0

import scodec._
import scodec.codecs._

final case class DescribeGroupsGroupMemberResponse(
  memberId:         Option[String],
  clientId:         Option[String],
  clientHost:       Option[String],
  memberMetadata:   Vector[Byte],
  memberAssignment: Vector[Byte]
)

final case class DescribeGroupsGroupResponse(
  errorCode:    KafkaError,
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
  implicit def codec(implicit kafkaError: Codec[KafkaError], member: Codec[DescribeGroupsGroupMemberResponse]): Codec[DescribeGroupsGroupResponse] = (
    ("errorCode" | kafkaError) ::
    ("groupId" | kafkaString) ::
    ("state" | kafkaString) ::
    ("protocolType" | kafkaString) ::
    ("protocol" | kafkaString) ::
    ("members" | kafkaArray(member))
  ).as[DescribeGroupsGroupResponse]

}