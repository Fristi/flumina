package vectos.kafka.types

import cats.Monad
import vectos.kafka.types.ir._

object kafka {

  trait Dsl[T] {
    def apply[F[_]: KafkaAlg]: F[T]
  }

  def listOffsets(topics: Set[TopicPartition]) = new Dsl[List[TopicPartitionResult[Vector[Long]]]] {
    override def apply[F[_]: KafkaAlg]: F[List[TopicPartitionResult[Vector[Long]]]] = implicitly[KafkaAlg[F]].listOffsets(topics)
  }
  def metadata(topics: Vector[String]) = new Dsl[Metadata] {
    override def apply[F[_]: KafkaAlg]: F[Metadata] = implicitly[KafkaAlg[F]].metadata(topics)
  }

  def offsetFetch(consumerGroup: String, topicPartitions: Set[TopicPartition]) = new Dsl[List[TopicPartitionResult[OffsetMetadata]]] {
    override def apply[F[_]: KafkaAlg]: F[List[TopicPartitionResult[OffsetMetadata]]] = implicitly[KafkaAlg[F]].offsetFetch(consumerGroup, topicPartitions)
  }

  def offsetCommit(consumerGroup: String, offsets: Map[TopicPartition, OffsetMetadata]) = new Dsl[List[TopicPartitionResult[Unit]]] {
    override def apply[F[_]: KafkaAlg]: F[List[TopicPartitionResult[Unit]]] = implicitly[KafkaAlg[F]].offsetCommit(consumerGroup, offsets)
  }

  def describeGroups(groupIds: Set[String]) = new Dsl[List[Group]] {
    override def apply[F[_]: KafkaAlg]: F[List[Group]] = implicitly[KafkaAlg[F]].describeGroups(groupIds)
  }

  def groupCoordinator(groupId: String) = new Dsl[GroupCoordinator] {
    override def apply[F[_]: KafkaAlg]: F[GroupCoordinator] = implicitly[KafkaAlg[F]].groupCoordinator(groupId)
  }

  def joinGroup(groupId: String, protocol: String, protocols: Seq[GroupProtocol]) = new Dsl[JoinGroupResult] {
    override def apply[F[_]: KafkaAlg]: F[JoinGroupResult] = implicitly[KafkaAlg[F]].joinGroup(groupId, protocol, protocols)
  }

  def leaveGroup(groupId: String, memberId: String) = new Dsl[Unit] {
    override def apply[F[_]: KafkaAlg]: F[Unit] = implicitly[KafkaAlg[F]].leaveGroup(groupId, memberId)
  }
  def heartbeat(groupId: String, generationId: Int, memberId: String) = new Dsl[Unit] {
    override def apply[F[_]: KafkaAlg]: F[Unit] = implicitly[KafkaAlg[F]].heartbeat(groupId, generationId, memberId)
  }

  def listGroups = new Dsl[List[GroupInfo]] {
    override def apply[F[_]: KafkaAlg]: F[List[GroupInfo]] = implicitly[KafkaAlg[F]].listGroups
  }

  def produce(values: Map[TopicPartition, List[(Array[Byte], Array[Byte])]]) = new Dsl[List[TopicPartitionResult[Long]]] {
    override def apply[F[_]: KafkaAlg]: F[List[TopicPartitionResult[Long]]] = implicitly[KafkaAlg[F]].produce(values)
  }

  def fetch(topicPartitionOffsets: Map[TopicPartition, Long]) = new Dsl[List[TopicPartitionResult[List[MessageEntry]]]] {
    override def apply[F[_]: KafkaAlg]: F[List[TopicPartitionResult[List[MessageEntry]]]] = implicitly[KafkaAlg[F]].fetch(topicPartitionOffsets)
  }

  implicit val monad: Monad[Dsl] = new Monad[Dsl] {
    def pure[A](x: A) = new Dsl[A] {
      override def apply[F[_]: KafkaAlg]: F[A] = implicitly[KafkaAlg[F]].pure(x)
    }
    def flatMap[A, B](fa: Dsl[A])(f: (A) => Dsl[B]) = new Dsl[B] {
      override def apply[F[_]: KafkaAlg]: F[B] = implicitly[KafkaAlg[F]].flatMap(fa.apply[F])(x => f(x).apply[F])
    }
  }
}
