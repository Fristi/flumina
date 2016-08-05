package vectos.kafka.types

import cats.Monad
import vectos.kafka.types.ir._

trait KafkaAlg[F[_]] extends Monad[F] {
  /** Discovery **/
  def metadata(topics: Set[String]): F[Metadata]
  def groupCoordinator(groupId: String): F[Broker]

  /** Producing and fetching **/
  def produce(values: Map[TopicPartition, List[Record]]): F[List[TopicPartitionResult[Long]]]
  def fetch(topicPartitionOffsets: Map[TopicPartition, Long]): F[List[TopicPartitionResult[List[RecordEntry]]]]
  def listOffsets(topics: Set[TopicPartition]): F[List[TopicPartitionResult[Vector[Long]]]]

  /** Stuff with groups **/
  def offsetFetch(groupId: String, topicPartitions: Set[TopicPartition]): F[List[TopicPartitionResult[OffsetMetadata]]]
  def offsetCommit(groupId: String, offsets: Map[TopicPartition, OffsetMetadata]): F[List[TopicPartitionResult[Unit]]]
  def describeGroups(groupIds: Set[String]): F[List[Group]]
  def joinGroup(groupId: String, protocol: String, protocols: Seq[GroupProtocol]): F[JoinGroupResult]
  def leaveGroup(groupId: String, memberId: String): F[Unit]
  def heartbeat(groupId: String, generationId: Int, memberId: String): F[Unit]
  def syncGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment]): F[MemberAssignment]
  def listGroups: F[List[GroupInfo]]
}