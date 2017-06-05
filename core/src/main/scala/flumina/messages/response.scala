package flumina.messages

import scodec._
import scodec.codecs._
import flumina._
import scodec.bits.ByteVector

sealed trait KafkaResponse

object KafkaResponse {

  final case class Produce(topics: Vector[ProduceTopicResponse], throttleTime: Int) extends KafkaResponse
  final case class Fetch(throttleTime: Int, topics: Vector[FetchTopicResponse]) extends KafkaResponse
  final case class Metadata(brokers: Vector[MetadataBrokerResponse], clusterId: Option[String], controllerId: Int, topicMetadata: Vector[MetadataTopicMetadataResponse]) extends KafkaResponse
  final case class OffsetCommit(topics: Vector[OffsetCommitTopicResponse]) extends KafkaResponse
  final case class OffsetFetch(topics: Vector[OffsetFetchTopicResponse]) extends KafkaResponse
  final case class GroupCoordinator(kafkaResult: KafkaResult, coordinatorId: Int, coordinatorHost: String, coordinatorPort: Int) extends KafkaResponse
  final case class JoinGroup(kafkaResult: KafkaResult, generationId: Int, groupProtocol: String, leaderId: String, memberId: String, members: Vector[JoinGroupMemberResponse]) extends KafkaResponse
  final case class Heartbeat(kafkaResult: KafkaResult) extends KafkaResponse
  final case class LeaveGroup(kafkaResult: KafkaResult) extends KafkaResponse
  final case class ListGroups(kafkaResult: KafkaResult, groups: Vector[ListGroupGroupResponse]) extends KafkaResponse
  final case class ApiVersions(kafkaResult: KafkaResult, versions: Vector[ApiVersion]) extends KafkaResponse
  final case class SyncGroup(result: KafkaResult, bytes: ByteVector) extends KafkaResponse
  final case class DescribeGroups(groups: Vector[DescribeGroupsGroupResponse]) extends KafkaResponse
  final case class CreateTopic(result: Vector[TopicResponse]) extends KafkaResponse
  final case class DeleteTopic(result: Vector[TopicResponse]) extends KafkaResponse

  val produce: Codec[Produce] =
    (("topics" | kafkaArray(ProduceTopicResponse.codec)) :: ("throttleTime" | int32)).as[Produce]

  val fetch: Codec[Fetch] =
    (("throttleTime" | int32) :: ("topics" | kafkaArray(FetchTopicResponse.codec))).as[Fetch]

  val metaData: Codec[Metadata] =
    (
      ("brokers" | kafkaArray(MetadataBrokerResponse.codec)) ::
      ("cluster_id" | kafkaOptionalString) ::
      ("controller_id" | int32) ::
      ("metadata" | kafkaArray(MetadataTopicMetadataResponse.codec))
    ).as[Metadata]

  val offsetCommit: Codec[OffsetCommit] =
    ("topics" | kafkaArray(OffsetCommitTopicResponse.codec)).as[OffsetCommit]

  val offsetFetch: Codec[OffsetFetch] =
    ("topics" | kafkaArray(OffsetFetchTopicResponse.codec)).as[OffsetFetch]

  val groupCoordinator: Codec[GroupCoordinator] =
    (("kafkaResult" | KafkaResult.codec) :: ("coordinatorId" | int32) :: ("coordinatorHost" | kafkaRequiredString) :: ("coordinatorPort" | int32)).as[GroupCoordinator]

  val joinGroup: Codec[JoinGroup] =
    (
      ("kafkaResult" | KafkaResult.codec) ::
      ("generationId" | int32) ::
      ("groupProtocol" | kafkaRequiredString) ::
      ("leaderId" | kafkaRequiredString) ::
      ("memberId" | kafkaRequiredString) ::
      ("members" | kafkaArray(JoinGroupMemberResponse.codec))
    ).as[JoinGroup]

  val heartbeat: Codec[Heartbeat] =
    ("kafkaResult" | KafkaResult.codec).as[Heartbeat]

  val leaveGroup: Codec[LeaveGroup] =
    ("kafkaResult" | KafkaResult.codec).as[LeaveGroup]

  val listGroups: Codec[ListGroups] =
    (("kafkaResult" | KafkaResult.codec) :: ("groups" | kafkaArray(ListGroupGroupResponse.codec))).as[ListGroups]

  val syncGroup: Codec[SyncGroup] =
    (("kafkaResult" | KafkaResult.codec) :: ("assignment" | kafkaBytes)).as[SyncGroup]

  val createTopic: Codec[CreateTopic] =
    ("topics" | kafkaArray(TopicResponse.codec)).as[CreateTopic]

  val deleteTopic: Codec[DeleteTopic] =
    ("topics" | kafkaArray(TopicResponse.codec)).as[DeleteTopic]

  val describeGroups: Codec[DescribeGroups] =
    ("groups" | kafkaArray(DescribeGroupsGroupResponse.codec)).as[DescribeGroups]

  val apiVersions: Codec[ApiVersions] =
    (("kafkaResult" | KafkaResult.codec) :: ("versions" | kafkaArray((int16 :: int16 :: int16).as[ApiVersion]))).as[ApiVersions]
}