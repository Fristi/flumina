package flumina.core.v090

import scodec._
import scodec.codecs._
import flumina.core._

sealed trait KafkaRequest

object KafkaRequest {

  final case class Produce(acks: Int, timeout: Int, topics: Vector[ProduceTopicRequest]) extends KafkaRequest
  final case class Fetch(replicaId: Int, maxWaitTime: Int, minBytes: Int, topics: Vector[FetchTopicRequest]) extends KafkaRequest
  final case class Metadata(topics: Vector[String]) extends KafkaRequest
  final case class OffsetCommit(groupId: String, generationId: Int, memberId: String, retentionTime: Long, topics: Vector[OffsetCommitTopicRequest]) extends KafkaRequest
  final case class OffsetFetch(groupId: String, topics: Vector[OffsetFetchTopicRequest]) extends KafkaRequest
  final case class GroupCoordinator(groupId: String) extends KafkaRequest
  final case class JoinGroup(groupId: String, sessionTimeOut: Int, memberId: String, protocolType: String, groupProtocols: Vector[JoinGroupProtocolRequest]) extends KafkaRequest
  final case class Heartbeat(groupId: String, generationId: Int, memberId: String) extends KafkaRequest
  final case class LeaveGroup(groupId: String, memberId: String) extends KafkaRequest
  final case object ListGroups extends KafkaRequest
  final case class SyncGroup(groupId: String, generationId: Int, memberId: String, groupAssignment: Vector[SyncGroupGroupAssignmentRequest]) extends KafkaRequest

  def produce(implicit topic: Codec[ProduceTopicRequest]): Codec[Produce] =
    (("acks" | int16) :: ("timeout" | int32) :: ("topics" | kafkaArray(topic))).as[Produce]

  def fetch(implicit topic: Codec[FetchTopicRequest]): Codec[Fetch] =
    (("replicaId" | int32) :: ("maxWaitTime" | int32) :: ("minBytes" | int32) :: ("topics" | kafkaArray(topic))).as[Fetch]

  def metaData: Codec[Metadata] =
    ("topics" | kafkaArray(kafkaRequiredString)).as[Metadata]

  def offsetCommit(implicit topic: Codec[OffsetCommitTopicRequest]): Codec[OffsetCommit] =
    (("groupId" | kafkaRequiredString) :: ("groupId" | int32) :: ("groupId" | kafkaRequiredString) :: ("retentionTime" | int64) :: ("topics" | kafkaArray(topic))).as[OffsetCommit]

  def offsetFetch(implicit topic: Codec[OffsetFetchTopicRequest]): Codec[OffsetFetch] =
    (("groupId" | kafkaRequiredString) :: ("topics" | kafkaArray(topic))).as[OffsetFetch]

  def groupCoordinator: Codec[GroupCoordinator] =
    ("groupId" | kafkaRequiredString).as[GroupCoordinator]

  def joinGroup(implicit groupProtocol: Codec[JoinGroupProtocolRequest]): Codec[JoinGroup] =
    (
      ("groupId" | kafkaRequiredString) ::
      ("sessionTimeOut" | int32) ::
      ("memberId" | kafkaRequiredString) ::
      ("protocolType" | kafkaRequiredString) ::
      ("groupProtocols" | kafkaArray(groupProtocol))
    ).as[JoinGroup]

  def heartbeat: Codec[Heartbeat] =
    (("groupId" | kafkaRequiredString) :: ("generationId" | int32) :: ("memberId" | kafkaRequiredString)).as[Heartbeat]

  def leaveGroup: Codec[LeaveGroup] =
    (("groupId" | kafkaRequiredString) :: ("memberId" | kafkaRequiredString)).as[LeaveGroup]

  def listGroups: Codec[ListGroups.type] =
    provide(ListGroups).as[ListGroups.type]

  def syncGroup(implicit assignment: Codec[SyncGroupGroupAssignmentRequest]): Codec[SyncGroup] =
    (
      ("groupId" | kafkaRequiredString) ::
      ("generationId" | int32) ::
      ("memberId" | kafkaRequiredString) ::
      ("groupAssignment" | kafkaArray(assignment))
    ).as[SyncGroup]

}

