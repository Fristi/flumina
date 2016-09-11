package flumina.core

import cats.Monad
import cats.data.Xor
import flumina.core.ir._

object kafka {

  trait Dsl[T] {
    def apply[F[_]: KafkaAlg]: F[T]
  }

  def metadata(topics: Set[String]) = new Dsl[Metadata] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].metadata(topics)
  }

  def offsetFetch(groupId: String, topicPartitions: Set[TopicPartition]) = new Dsl[TopicPartitionResults[OffsetMetadata]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].offsetFetch(groupId, topicPartitions)
  }

  def offsetCommit(groupId: String, generationId: Int, memberId: String, offsets: Map[TopicPartition, OffsetMetadata]) = new Dsl[TopicPartitionResults[Unit]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].offsetCommit(groupId, generationId, memberId, offsets)
  }

  def groupCoordinator(groupId: String) = new Dsl[KafkaResult Xor Broker] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].groupCoordinator(groupId)
  }

  def joinGroup(groupId: String, memberId: Option[String], protocol: String, protocols: Seq[GroupProtocol]) = new Dsl[KafkaResult Xor JoinGroupResult] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].joinGroup(groupId, memberId, protocol, protocols)
  }

  def syncGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment]) = new Dsl[KafkaResult Xor MemberAssignment] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].syncGroup(groupId, generationId, memberId, assignments)
  }

  def leaveGroup(groupId: String, memberId: String) = new Dsl[KafkaResult Xor Unit] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].leaveGroup(groupId, memberId)
  }
  def heartbeat(groupId: String, generationId: Int, memberId: String) = new Dsl[KafkaResult Xor Unit] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].heartbeat(groupId, generationId, memberId)
  }

  def listGroups = new Dsl[KafkaResult Xor List[GroupInfo]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].listGroups
  }

  def produce(values: List[(TopicPartition, Record)]) = new Dsl[TopicPartitionResults[Long]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].produce(values)
  }

  def fetch(topicPartitionOffsets: Map[TopicPartition, Long]) = new Dsl[TopicPartitionResults[List[RecordEntry]]] {
    override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].fetch(topicPartitionOffsets)
  }

  implicit val monad: Monad[Dsl] = new Monad[Dsl] {
    def pure[A](x: A) = new Dsl[A] {
      override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].pure(x)
    }
    def flatMap[A, B](fa: Dsl[A])(f: (A) => Dsl[B]) = new Dsl[B] {
      override def apply[F[_]: KafkaAlg] = implicitly[KafkaAlg[F]].flatMap(fa.apply[F])(x => f(x).apply[F])
    }

    override def tailRecM[A, B](a: A)(f: (A) => Dsl[Either[A, B]]): Dsl[B] = flatMap(f(a)) {
      case Left(ohh)  => tailRecM(ohh)(f)
      case Right(ohh) => pure(ohh)
    }
  }
}
