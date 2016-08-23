package flumina.types

import cats.Monad
import flumina.types.ir._

object kafka {

  trait Dsl[T] {
    def apply[F[_]: KafkaAlg]: F[T]
  }

  def listOffsets(topics: Set[TopicPartition]) = new Dsl[List[TopicPartitionResult[Vector[Long]]]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].listOffsets(topics)
  }
  def metadata(topics: Set[String]) = new Dsl[Metadata] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].metadata(topics)
  }

  def offsetFetch(groupId: String, topicPartitions: Set[TopicPartition]) = new Dsl[List[TopicPartitionResult[OffsetMetadata]]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].offsetFetch(groupId, topicPartitions)
  }

  def offsetCommit(groupId: String, offsets: Map[TopicPartition, OffsetMetadata]) = new Dsl[List[TopicPartitionResult[Unit]]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].offsetCommit(groupId, offsets)
  }

  def describeGroups(groupIds: Set[String]) = new Dsl[List[Group]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].describeGroups(groupIds)
  }

  def groupCoordinator(groupId: String) = new Dsl[Broker] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].groupCoordinator(groupId)
  }

  def joinGroup(groupId: String, protocol: String, protocols: Seq[GroupProtocol]) = new Dsl[JoinGroupResult] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].joinGroup(groupId, protocol, protocols)
  }

  def syncGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment]) = new Dsl[MemberAssignment] {
    override def apply[F[_]: KafkaAlg]: F[MemberAssignment] = implicitly[KafkaAlg[F]].syncGroup(groupId, generationId, memberId, assignments)
  }

  def leaveGroup(groupId: String, memberId: String) = new Dsl[Unit] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].leaveGroup(groupId, memberId)
  }
  def heartbeat(groupId: String, generationId: Int, memberId: String) = new Dsl[Unit] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].heartbeat(groupId, generationId, memberId)
  }

  def listGroups = new Dsl[List[GroupInfo]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].listGroups
  }

  def produce(values: Map[TopicPartition, List[Record]]) = new Dsl[List[TopicPartitionResult[Long]]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].produce(values)
  }

  def fetch(topicPartitionOffsets: Map[TopicPartition, Long]) = new Dsl[List[TopicPartitionResult[List[RecordEntry]]]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].fetch(topicPartitionOffsets)
  }

  implicit val monad: Monad[Dsl] = new Monad[Dsl] {
    def pure[A](x: A) = new Dsl[A] {
      override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].pure(x)
    }
    def flatMap[A, B](fa: Dsl[A])(f: (A) => Dsl[B]) = new Dsl[B] {
      override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].flatMap(fa.apply[F])(x => f(x).apply[F])
    }
  }
}
