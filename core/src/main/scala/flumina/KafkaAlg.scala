package flumina

import cats.free.Free

sealed trait KafkaA[A]

object KafkaA {
  final case class OffsetsFetch(groupId: String, values: Set[TopicPartition])                                                        extends KafkaA[TopicPartitionValues[OffsetMetadata]]
  final case class OffsetsCommit(groupId: String, generationId: Int, memberId: String, offsets: Map[TopicPartition, OffsetMetadata]) extends KafkaA[TopicPartitionValues[Unit]]
  final case class ProduceN(compression: Compression, values: Traversable[TopicPartitionValue[Record]])                              extends KafkaA[TopicPartitionValues[Long]]
  final case class ProduceOne(values: TopicPartitionValue[Record])                                                                   extends KafkaA[TopicPartitionValues[Long]]
  final case class JoinGroup(groupId: String, memberId: Option[String], protocol: String, protocols: Seq[GroupProtocol])             extends KafkaA[Either[KafkaResult, JoinGroupResult]]
  final case class DescribeGroups(groupIds: Traversable[String])                                                                     extends KafkaA[Seq[Group]]
  final case class SynchronizeGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment])         extends KafkaA[Either[KafkaResult, MemberAssignment]]
  final case class Heartbeat(groupId: String, generationId: Int, memberId: String)                                                   extends KafkaA[Either[KafkaResult, Unit]]
  final case class Fetch(values: Traversable[TopicPartitionValue[Long]])                                                             extends KafkaA[TopicPartitionValues[OffsetValue[Record]]]
  final case class LeaveGroup(groupId: String, memberId: String)                                                                     extends KafkaA[Either[KafkaResult, Unit]]
  final case class GroupCoordinator(groupId: String)                                                                                 extends KafkaA[Either[KafkaResult, Broker]]
  final case class GetMetadata(topics: Traversable[String])                                                                          extends KafkaA[Metadata]
  final case class CreateTopics(topics: Traversable[TopicDescriptor])                                                                extends KafkaA[List[TopicResult]]
  final case class DeleteTopics(topics: Traversable[String])                                                                         extends KafkaA[List[TopicResult]]
  final case object ListGroups                                                                                                       extends KafkaA[Either[KafkaResult, List[GroupInfo]]]
  final case object ApiVersions                                                                                                      extends KafkaA[Either[KafkaResult, List[ApiVersion]]]
}

object kafka {
  def createTopics(topics: Set[TopicDescriptor]): Free[KafkaA, List[TopicResult]] =
    Free.liftF(KafkaA.CreateTopics(topics))

  def deleteTopics(topics: Set[String]): Free[KafkaA, List[TopicResult]] =
    Free.liftF(KafkaA.DeleteTopics(topics))

  def metadata(topics: Set[String]): Free[KafkaA, Metadata] =
    Free.liftF(KafkaA.GetMetadata(topics))

  def groupCoordinator(groupId: String): Free[KafkaA, KafkaResult Either Broker] =
    Free.liftF(KafkaA.GroupCoordinator(groupId))

  def produceOne(values: TopicPartitionValue[Record]): Free[KafkaA, TopicPartitionValues[Long]] =
    Free.liftF(KafkaA.ProduceOne(values))

  def produceN(compression: Compression, values: Seq[TopicPartitionValue[Record]]): Free[KafkaA, TopicPartitionValues[Long]] =
    Free.liftF(KafkaA.ProduceN(compression, values))

  def fetch(topicPartitionOffsets: Set[TopicPartitionValue[Long]]): Free[KafkaA, TopicPartitionValues[OffsetValue[Record]]] =
    Free.liftF(KafkaA.Fetch(topicPartitionOffsets))

  def offsetFetch(groupId: String, topicPartitions: Set[TopicPartition]): Free[KafkaA, TopicPartitionValues[OffsetMetadata]] =
    Free.liftF(KafkaA.OffsetsFetch(groupId, topicPartitions))

  def offsetCommit(groupId: String, generationId: Int, memberId: String, offsets: Map[TopicPartition, OffsetMetadata]): Free[KafkaA, TopicPartitionValues[Unit]] =
    Free.liftF(KafkaA.OffsetsCommit(groupId, generationId, memberId, offsets))

  def joinGroup(groupId: String, memberId: Option[String], protocol: String, protocols: Seq[GroupProtocol]): Free[KafkaA, KafkaResult Either JoinGroupResult] =
    Free.liftF(KafkaA.JoinGroup(groupId, memberId, protocol, protocols))

  def leaveGroup(groupId: String, memberId: String): Free[KafkaA, Either[KafkaResult, Unit]] =
    Free.liftF(KafkaA.LeaveGroup(groupId, memberId))

  def heartbeat(groupId: String, generationId: Int, memberId: String): Free[KafkaA, Either[KafkaResult, Unit]] =
    Free.liftF(KafkaA.Heartbeat(groupId, generationId, memberId))

  def syncGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment]): Free[KafkaA, KafkaResult Either MemberAssignment] =
    Free.liftF(KafkaA.SynchronizeGroup(groupId, generationId, memberId, assignments))

  def apiVersions: Free[KafkaA, KafkaResult Either List[ApiVersion]] =
    Free.liftF(KafkaA.ApiVersions)

  def listGroups: Free[KafkaA, KafkaResult Either List[GroupInfo]] =
    Free.liftF(KafkaA.ListGroups)

  def describeGroups(groupIds: Set[String]): Free[KafkaA, Seq[Group]] =
    Free.liftF(KafkaA.DescribeGroups(groupIds))
}
