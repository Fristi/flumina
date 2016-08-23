package flumina.types.v0

import scodec._
import scodec.codecs._
import flumina.types._

sealed trait KafkaResponse

object KafkaResponse {

  final case class Produce(topics: Vector[ProduceTopicResponse]) extends KafkaResponse
  final case class Fetch(topics: Vector[FetchTopicResponse]) extends KafkaResponse
  final case class ListOffset(topics: Vector[ListOffsetTopicResponse]) extends KafkaResponse
  final case class Metadata(brokers: Vector[MetadataBrokerResponse], topicMetadata: Vector[MetadataTopicMetadataResponse]) extends KafkaResponse
  final case class OffsetCommit(topics: Vector[OffsetCommitTopicResponse]) extends KafkaResponse
  final case class OffsetFetch(topics: Vector[OffsetFetchTopicResponse]) extends KafkaResponse
  final case class GroupCoordinator(kafkaResult: KafkaResult, coordinatorId: Int, coordinatorHost: Option[String], coordinatorPort: Int) extends KafkaResponse
  final case class JoinGroup(kafkaResult: KafkaResult, generationId: Int, groupProtocol: Option[String], leaderId: Option[String], memberId: Option[String], members: Vector[JoinGroupMemberResponse]) extends KafkaResponse
  final case class Heartbeat(kafkaResult: KafkaResult) extends KafkaResponse
  final case class LeaveGroup(kafkaResult: KafkaResult) extends KafkaResponse
  final case class ListGroups(kafkaResult: KafkaResult, groups: Vector[ListGroupGroupResponse]) extends KafkaResponse
  final case class DescribeGroups(groups: Vector[DescribeGroupsGroupResponse]) extends KafkaResponse
  final case class SyncGroup(result: KafkaResult, bytes: Vector[Byte]) extends KafkaResponse

  def produce(implicit topic: Codec[ProduceTopicResponse]): Codec[Produce] =
    ("topics" | kafkaArray(topic)).as[Produce]

  def fetch(implicit topic: Codec[FetchTopicResponse]): Codec[Fetch] =
    ("topics" | kafkaArray(topic)).as[Fetch]

  def listOffset(implicit topic: Codec[ListOffsetTopicResponse]): Codec[ListOffset] =
    ("topics" | kafkaArray(topic)).as[ListOffset]

  def metaData(implicit brokers: Codec[MetadataBrokerResponse], metadata: Codec[MetadataTopicMetadataResponse]): Codec[Metadata] =
    (("brokers" | kafkaArray(brokers)) :: ("metadata" | kafkaArray(metadata))).as[Metadata]

  def offsetCommit(implicit topic: Codec[OffsetCommitTopicResponse]): Codec[OffsetCommit] =
    ("topics" | kafkaArray(topic)).as[OffsetCommit]

  def offsetFetch(implicit topic: Codec[OffsetFetchTopicResponse]): Codec[OffsetFetch] =
    ("topics" | kafkaArray(topic)).as[OffsetFetch]

  def groupCoordinator(implicit kafkaResult: Codec[KafkaResult]): Codec[GroupCoordinator] =
    (("kafkaResult" | kafkaResult) :: ("coordinatorId" | int32) :: ("coordinatorHost" | kafkaString) :: ("coordinatorPort" | int32)).as[GroupCoordinator]

  def joinGroup(implicit kafkaResult: Codec[KafkaResult], member: Codec[JoinGroupMemberResponse]): Codec[JoinGroup] =
    (
      ("kafkaResult" | kafkaResult) ::
      ("generationId" | int32) ::
      ("groupProtocol" | kafkaString) ::
      ("leaderId" | kafkaString) ::
      ("memberId" | kafkaString) ::
      ("members" | kafkaArray(member))
    ).as[JoinGroup]

  def heartbeat(implicit kafkaResult: Codec[KafkaResult]): Codec[Heartbeat] =
    ("kafkaResult" | kafkaResult).as[Heartbeat]

  def leaveGroup(implicit kafkaResult: Codec[KafkaResult]): Codec[LeaveGroup] =
    ("kafkaResult" | kafkaResult).as[LeaveGroup]

  def listGroups(implicit kafkaResult: Codec[KafkaResult], group: Codec[ListGroupGroupResponse]): Codec[ListGroups] =
    (("kafkaResult" | kafkaResult) :: ("groups" | kafkaArray(group))).as[ListGroups]

  def describeGroups(implicit group: Codec[DescribeGroupsGroupResponse]): Codec[DescribeGroups] =
    ("groups" | kafkaArray(group)).as[DescribeGroups]

  def syncGroup(implicit kafkaResult: Codec[KafkaResult], assignment: Codec[MemberAssignmentData]): Codec[SyncGroup] =
    (("kafkaResult" | kafkaResult) :: ("assignment" | kafkaBytes)).as[SyncGroup]
}