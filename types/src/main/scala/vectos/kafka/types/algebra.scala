package vectos.kafka.types

import cats.Monad
import vectos.kafka.types.ir._

trait KafkaAlg[F[_]] extends Monad[F] {
  def listOffsets(topics: Set[TopicPartition]): F[List[TopicPartitionResult[Vector[Long]]]]
  def metadata(topics: Vector[String]): F[Metadata]

  def offsetFetch(consumerGroup: String, topicPartitions: Set[TopicPartition]): F[List[TopicPartitionResult[OffsetMetadata]]]
  def offsetCommit(consumerGroup: String, offsets: Map[TopicPartition, Long]): F[List[TopicPartitionResult[Unit]]]

  def describeGroups(groupIds: Set[String]): F[List[Group]]
  def groupCoordinator(groupId: String): F[GroupCoordinator]
  def joinGroup(groupId: String, protocol: String, protocols: Seq[JoinGroupProtocol]): F[JoinGroupResult]
  def leaveGroup(groupId: String, memberId: String): F[Unit]
  def heartbeat(groupId: String, generationId: Int, memberId: String): F[Unit]
  def listGroups: F[List[GroupInfo]]

  def produce(values: Map[TopicPartition, List[(Array[Byte], Array[Byte])]]): F[List[TopicPartitionResult[Long]]]
  def fetch(topicPartitionOffsets: Map[TopicPartition, Long]): F[List[TopicPartitionResult[List[MessageEntry]]]]

}