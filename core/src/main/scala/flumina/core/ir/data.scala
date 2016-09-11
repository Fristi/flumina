package flumina.core.ir

import cats.implicits._
import cats.data.Ior
import cats.{Monoid, Semigroup}
import flumina.core.KafkaResult
import scodec.bits.ByteVector

final case class Record(key: ByteVector, value: ByteVector)

final case class RecordEntry(offset: Long, record: Record)

final case class TopicPartition(topic: String, partition: Int)

final case class TopicPartitionRecordEntry(topicPartition: TopicPartition, recordEntry: RecordEntry)

object TopicPartition {
  def enumerate(name: String, nrPartitions: Int) = (0 until nrPartitions).map(x => TopicPartition(name, x)).toSet
}

final case class GroupInfo(groupId: String, protocolType: String)

final case class OffsetMetadata(offset: Long, metadata: Option[String])

final case class Broker(nodeId: Int, host: String, port: Int)

final case class TopicInfo(leader: Int, replicas: Seq[Int], isr: Seq[Int])

final case class TopicResult(topic: String, kafkaResult: KafkaResult)

final case class Metadata(brokers: Set[Broker], topics: Option[Ior[Set[TopicResult], Set[TopicPartitionResult[TopicInfo]]]]) {
  val topicsWhichCanBeRetried: Set[String] = topics.map {
    case Ior.Right(_)      => Set.empty[String]
    case Ior.Left(left)    => left.map(_.topic)
    case Ior.Both(left, _) => left.map(_.topic)
  } getOrElse {
    Set.empty[String]
  }

  def withoutRetryErrors = {
    def withoutRetrieableErrors(t: Ior[Set[TopicResult], Set[TopicPartitionResult[TopicInfo]]]) = t match {
      case Ior.Left(left)        => Ior.left(left.filterNot(x => KafkaResult.canRetry(x.kafkaResult)))
      case Ior.Right(right)      => Ior.right(right)
      case Ior.Both(left, right) => Ior.both(left.filterNot(x => KafkaResult.canRetry(x.kafkaResult)), right)
    }

    Metadata(brokers, topics.map(withoutRetrieableErrors))
  }
}

object Metadata {
  implicit val semigroup: Semigroup[Metadata] = new Semigroup[Metadata] {
    def combine(x: Metadata, y: Metadata) = Metadata(x.brokers |+| y.brokers, x.topics |+| y.topics)
  }
}

final case class GroupMember(
  memberId:         String,
  clientId:         Option[String],
  clientHost:       Option[String],
  consumerProtocol: Option[ConsumerProtocol],
  assignment:       Option[MemberAssignment]
)

final case class GroupProtocol(protocolName: String, consumerProtocols: Seq[ConsumerProtocol])

final case class JoinGroupResult(generationId: Int, groupProtocol: String, leaderId: String, memberId: String, members: Seq[GroupMember])

final case class Group(
  kafkaResult:  KafkaResult,
  groupId:      String,
  state:        String,
  protocolType: String,
  protocol:     String,
  members:      Seq[GroupMember]
)

final case class GroupAssignment(memberId: String, memberAssignment: MemberAssignment)

final case class MemberAssignment(version: Int, topicPartitions: Seq[TopicPartition], userData: ByteVector)

final case class ConsumerProtocol(version: Int, subscriptions: Seq[String], userData: ByteVector)

final case class TopicPartitionResult[T](topicPartition: TopicPartition, result: T)

final case class TopicPartitionResults[T](errors: List[TopicPartitionResult[KafkaResult]], success: List[TopicPartitionResult[T]]) {
  lazy val canBeRetried = errors.filter(x => KafkaResult.canRetry(x.result)).map(_.topicPartition).toSet
  def resultsExceptWhichCanBeRetried = TopicPartitionResults(errors.filterNot(x => KafkaResult.canRetry(x.result)), success)
}

object TopicPartitionResults {
  def zero[A] = TopicPartitionResults(List.empty, List.empty[TopicPartitionResult[A]])

  implicit def monoid[A]: Monoid[TopicPartitionResults[A]] = new Monoid[TopicPartitionResults[A]] {
    def empty = zero[A]
    def combine(x: TopicPartitionResults[A], y: TopicPartitionResults[A]) = TopicPartitionResults(x.errors ++ y.errors, x.success ++ y.success)
  }

  def from[A, B](xs: Seq[(KafkaResult, TopicPartition, A)]) = {
    val (errors, success) = xs.partition(x => x._1 != KafkaResult.NoError)

    TopicPartitionResults(errors.map(e => TopicPartitionResult(e._2, e._1)).toList, success.map(x => TopicPartitionResult(x._2, x._3)).toList)
  }
}