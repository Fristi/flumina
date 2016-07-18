package vectos.kafka.types

import cats.Monad
import vectos.kafka.types.ir._

object dsl {

  trait KafkaDsl[T] {
    def apply[F[_]: KafkaAlg]: F[T]
  }

  def listOffsets(topics: Set[TopicPartition]) = new KafkaDsl[List[TopicPartitionResult[Vector[Long]]]] {
    override def apply[F[_]: KafkaAlg]: F[List[TopicPartitionResult[Vector[Long]]]] = implicitly[KafkaAlg[F]].listOffsets(topics)
  }
  def metadata(topics: Vector[String]) = new KafkaDsl[Metadata] {
    override def apply[F[_]: KafkaAlg]: F[Metadata] = implicitly[KafkaAlg[F]].metadata(topics)
  }

  def offsetFetch(consumerGroup: String, topicPartitions: Set[TopicPartition]) = new KafkaDsl[List[TopicPartitionResult[OffsetMetadata]]] {
    override def apply[F[_]: KafkaAlg]: F[List[TopicPartitionResult[OffsetMetadata]]] = implicitly[KafkaAlg[F]].offsetFetch(consumerGroup, topicPartitions)
  }

  def offsetCommit(consumerGroup: String, offsets: Map[TopicPartition, Long]) = new KafkaDsl[List[TopicPartitionResult[Unit]]] {
    override def apply[F[_]: KafkaAlg]: F[List[TopicPartitionResult[Unit]]] = implicitly[KafkaAlg[F]].offsetCommit(consumerGroup, offsets)
  }

  def describeGroups(groupIds: Set[String]) = new KafkaDsl[List[Group]] {
    override def apply[F[_]: KafkaAlg]: F[List[Group]] = implicitly[KafkaAlg[F]].describeGroups(groupIds)
  }

  def groupCoordinator(groupId: String) = new KafkaDsl[GroupCoordinator] {
    override def apply[F[_]: KafkaAlg]: F[GroupCoordinator] = implicitly[KafkaAlg[F]].groupCoordinator(groupId)
  }

  def joinGroup(groupId: String, protocol: String, protocols: Seq[JoinGroupProtocol]) = new KafkaDsl[JoinGroupResult] {
    override def apply[F[_]: KafkaAlg]: F[JoinGroupResult] = implicitly[KafkaAlg[F]].joinGroup(groupId, protocol, protocols)
  }

  def leaveGroup(groupId: String, memberId: String) = new KafkaDsl[Unit] {
    override def apply[F[_]: KafkaAlg]: F[Unit] = implicitly[KafkaAlg[F]].leaveGroup(groupId, memberId)
  }
  def heartbeat(groupId: String, generationId: Int, memberId: String) = new KafkaDsl[Unit] {
    override def apply[F[_]: KafkaAlg]: F[Unit] = implicitly[KafkaAlg[F]].heartbeat(groupId, generationId, memberId)
  }

  def listGroups = new KafkaDsl[List[GroupInfo]] {
    override def apply[F[_]: KafkaAlg]: F[List[GroupInfo]] = implicitly[KafkaAlg[F]].listGroups
  }

  def produce(values: Map[TopicPartition, List[(Array[Byte], Array[Byte])]]) = new KafkaDsl[List[TopicPartitionResult[Long]]] {
    override def apply[F[_]: KafkaAlg]: F[List[TopicPartitionResult[Long]]] = implicitly[KafkaAlg[F]].produce(values)
  }

  def fetch(topicPartitionOffsets: Map[TopicPartition, Long]) = new KafkaDsl[List[TopicPartitionResult[List[MessageEntry]]]] {
    override def apply[F[_]: KafkaAlg]: F[List[TopicPartitionResult[List[MessageEntry]]]] = implicitly[KafkaAlg[F]].fetch(topicPartitionOffsets)
  }

  implicit val monad: Monad[KafkaDsl] = new Monad[KafkaDsl] {
    def pure[A](x: A) = new KafkaDsl[A] {
      override def apply[F[_]: KafkaAlg]: F[A] = implicitly[KafkaAlg[F]].pure(x)
    }
    def flatMap[A, B](fa: KafkaDsl[A])(f: (A) => KafkaDsl[B]) = new KafkaDsl[B] {
      override def apply[F[_]: KafkaAlg]: F[B] = implicitly[KafkaAlg[F]].flatMap(fa.apply[F])(x => f(x).apply[F])
    }
  }
}
