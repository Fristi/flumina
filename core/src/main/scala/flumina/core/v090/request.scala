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
  final case class CreateTopic(topics: Vector[CreateTopicRequest], timeout: Int) extends KafkaRequest
  final case class DeleteTopic(topics: Vector[String], timeout: Int) extends KafkaRequest

  val produce: Codec[Produce] =
    (("acks" | int16) :: ("timeout" | int32) :: ("topics" | kafkaArray(ProduceTopicRequest.codec))).as[Produce]

  val fetch: Codec[Fetch] =
    (("replicaId" | int32) :: ("maxWaitTime" | int32) :: ("minBytes" | int32) :: ("topics" | kafkaArray(FetchTopicRequest.codec))).as[Fetch]

  val metaData: Codec[Metadata] =
    ("topics" | kafkaNullableArray(kafkaRequiredString)).as[Metadata]

  val offsetCommit: Codec[OffsetCommit] =
    (("groupId" | kafkaRequiredString) :: ("groupId" | int32) :: ("groupId" | kafkaRequiredString) :: ("retentionTime" | int64) :: ("topics" | kafkaArray(OffsetCommitTopicRequest.codec))).as[OffsetCommit]

  val offsetFetch: Codec[OffsetFetch] =
    (("groupId" | kafkaRequiredString) :: ("topics" | kafkaArray(OffsetFetchTopicRequest.codec))).as[OffsetFetch]

  val groupCoordinator: Codec[GroupCoordinator] =
    ("groupId" | kafkaRequiredString).as[GroupCoordinator]

  val joinGroup: Codec[JoinGroup] =
    (
      ("groupId" | kafkaRequiredString) ::
      ("sessionTimeOut" | int32) ::
      ("memberId" | kafkaRequiredString) ::
      ("protocolType" | kafkaRequiredString) ::
      ("groupProtocols" | kafkaArray(JoinGroupProtocolRequest.codec))
    ).as[JoinGroup]

  val heartbeat: Codec[Heartbeat] =
    (("groupId" | kafkaRequiredString) :: ("generationId" | int32) :: ("memberId" | kafkaRequiredString)).as[Heartbeat]

  val leaveGroup: Codec[LeaveGroup] =
    (("groupId" | kafkaRequiredString) :: ("memberId" | kafkaRequiredString)).as[LeaveGroup]

  val listGroups: Codec[ListGroups.type] =
    provide(ListGroups).as[ListGroups.type]

  val syncGroup: Codec[SyncGroup] =
    (
      ("groupId" | kafkaRequiredString) ::
      ("generationId" | int32) ::
      ("memberId" | kafkaRequiredString) ::
      ("groupAssignment" | kafkaArray(SyncGroupGroupAssignmentRequest.codec))
    ).as[SyncGroup]

  val createTopic: Codec[CreateTopic] =
    (("topics" | kafkaArray(CreateTopicRequest.codec)) :: ("timeout" | int32)).as[CreateTopic]

  val deleteTopic: Codec[DeleteTopic] =
    (("topics" | kafkaArray(kafkaRequiredString)) :: ("timeout" | int32)).as[DeleteTopic]
}

