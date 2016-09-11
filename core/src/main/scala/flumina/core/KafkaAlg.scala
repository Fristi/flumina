package flumina.core

import cats.Monad
import cats.data.Xor
import flumina.core.ir._

trait KafkaAlg[F[_]] extends Monad[F] {
  /** Discovery **/
  def metadata(topics: Set[String]): F[Metadata]
  def groupCoordinator(groupId: String): F[KafkaResult Xor Broker]

  /** Producing and fetching **/
  def produce(values: List[(TopicPartition, Record)]): F[TopicPartitionResults[Long]]
  def fetch(topicPartitionOffsets: Map[TopicPartition, Long]): F[TopicPartitionResults[List[RecordEntry]]]

  /** Stuff with groups **/
  def offsetFetch(groupId: String, topicPartitions: Set[TopicPartition]): F[TopicPartitionResults[OffsetMetadata]]
  def offsetCommit(groupId: String, generationId: Int, memberId: String, offsets: Map[TopicPartition, OffsetMetadata]): F[TopicPartitionResults[Unit]]

  def joinGroup(groupId: String, memberId: Option[String], protocol: String, protocols: Seq[GroupProtocol]): F[KafkaResult Xor JoinGroupResult]
  def leaveGroup(groupId: String, memberId: String): F[KafkaResult Xor Unit]
  def heartbeat(groupId: String, generationId: Int, memberId: String): F[KafkaResult Xor Unit]
  def syncGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment]): F[KafkaResult Xor MemberAssignment]
  def listGroups: F[KafkaResult Xor List[GroupInfo]]
}