package flumina.core.v090

import scodec._
import scodec.codecs._
import flumina.core._
import scodec.bits.ByteVector

sealed trait KafkaResponse

object KafkaResponse {

  final case class Produce(topics: Vector[ProduceTopicResponse], throttleTime: Int) extends KafkaResponse
  final case class Fetch(throttleTime: Int, topics: Vector[FetchTopicResponse]) extends KafkaResponse
  final case class Metadata(brokers: Vector[MetadataBrokerResponse], topicMetadata: Vector[MetadataTopicMetadataResponse]) extends KafkaResponse
  final case class OffsetCommit(topics: Vector[OffsetCommitTopicResponse]) extends KafkaResponse
  final case class OffsetFetch(topics: Vector[OffsetFetchTopicResponse]) extends KafkaResponse
  final case class GroupCoordinator(kafkaResult: KafkaResult, coordinatorId: Int, coordinatorHost: String, coordinatorPort: Int) extends KafkaResponse
  final case class JoinGroup(kafkaResult: KafkaResult, generationId: Int, groupProtocol: String, leaderId: String, memberId: String, members: Vector[JoinGroupMemberResponse]) extends KafkaResponse
  final case class Heartbeat(kafkaResult: KafkaResult) extends KafkaResponse
  final case class LeaveGroup(kafkaResult: KafkaResult) extends KafkaResponse
  final case class ListGroups(kafkaResult: KafkaResult, groups: Vector[ListGroupGroupResponse]) extends KafkaResponse
  final case class SyncGroup(result: KafkaResult, bytes: ByteVector) extends KafkaResponse

  def produce(implicit topic: Codec[ProduceTopicResponse]): Codec[Produce] =
    (("topics" | kafkaArray(topic)) :: ("throttleTime" | int32)).as[Produce]

  def fetch(implicit topic: Codec[FetchTopicResponse]): Codec[Fetch] =
    (("throttleTime" | int32) :: ("topics" | kafkaArray(topic))).as[Fetch]

  def metaData(implicit brokers: Codec[MetadataBrokerResponse], metadata: Codec[MetadataTopicMetadataResponse]): Codec[Metadata] =
    (("brokers" | kafkaArray(brokers)) :: ("metadata" | kafkaArray(metadata))).as[Metadata]

  def offsetCommit(implicit topic: Codec[OffsetCommitTopicResponse]): Codec[OffsetCommit] =
    ("topics" | kafkaArray(topic)).as[OffsetCommit]

  def offsetFetch(implicit topic: Codec[OffsetFetchTopicResponse]): Codec[OffsetFetch] =
    ("topics" | kafkaArray(topic)).as[OffsetFetch]

  def groupCoordinator(implicit kafkaResult: Codec[KafkaResult]): Codec[GroupCoordinator] =
    (("kafkaResult" | kafkaResult) :: ("coordinatorId" | int32) :: ("coordinatorHost" | kafkaRequiredString) :: ("coordinatorPort" | int32)).as[GroupCoordinator]

  def joinGroup(implicit kafkaResult: Codec[KafkaResult], member: Codec[JoinGroupMemberResponse]): Codec[JoinGroup] =
    (
      ("kafkaResult" | kafkaResult) ::
      ("generationId" | int32) ::
      ("groupProtocol" | kafkaRequiredString) ::
      ("leaderId" | kafkaRequiredString) ::
      ("memberId" | kafkaRequiredString) ::
      ("members" | kafkaArray(member))
    ).as[JoinGroup]

  def heartbeat(implicit kafkaResult: Codec[KafkaResult]): Codec[Heartbeat] =
    ("kafkaResult" | kafkaResult).as[Heartbeat]

  def leaveGroup(implicit kafkaResult: Codec[KafkaResult]): Codec[LeaveGroup] =
    ("kafkaResult" | kafkaResult).as[LeaveGroup]

  def listGroups(implicit kafkaResult: Codec[KafkaResult], group: Codec[ListGroupGroupResponse]): Codec[ListGroups] =
    (("kafkaResult" | kafkaResult) :: ("groups" | kafkaArray(group))).as[ListGroups]

  def syncGroup(implicit kafkaResult: Codec[KafkaResult], assignment: Codec[MemberAssignmentData]): Codec[SyncGroup] =
    (("kafkaResult" | kafkaResult) :: ("assignment" | kafkaBytes)).as[SyncGroup]
}