package flumina.core.ir

import cats.Monoid
import flumina.core.KafkaResult
import scodec.bits.ByteVector
import scodec.{Attempt, Err}

abstract class Compression(val id: Int)

object Compression {
  def apply(id: Int): Attempt[Compression] = id match {
    case 1 => Attempt.successful(GZIP)
    case 2 => Attempt.successful(Snappy)
    case _ => Attempt.failure(Err("Compression not recognized"))
  }

  case object GZIP extends Compression(1)
  case object Snappy extends Compression(2)
}

abstract class MessageVersion(val id: Int)

object MessageVersion {
  case object V0 extends MessageVersion(0)
  case object V1 extends MessageVersion(1)
}

final case class ReplicationAssignment(partitionId: Int, replicas: Seq[Int])

final case class TopicDescriptor(
  topic:             String,
  nrPartitions:      Option[Int],
  replicationFactor: Option[Int],
  replicaAssignment: Seq[ReplicationAssignment],
  config:            Map[String, String]
)

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

final case class Metadata(
  brokers:       Set[Broker],
  controller:    Broker,
  topics:        Set[TopicPartitionValue[TopicInfo]],
  topicsInError: Set[TopicResult]
)

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

final case class TopicPartitionValue[T](topicPartition: TopicPartition, result: T)

final case class TopicPartitionValues[T](errors: List[TopicPartitionValue[KafkaResult]], success: List[TopicPartitionValue[T]]) {
  lazy val canBeRetried = errors.filter(x => KafkaResult.canRetry(x.result)).map(_.topicPartition).toSet
  def resultsExceptWhichCanBeRetried = TopicPartitionValues(errors.filterNot(x => KafkaResult.canRetry(x.result)), success)
}

object TopicPartitionValues {
  def zero[A] = TopicPartitionValues(List.empty, List.empty[TopicPartitionValue[A]])

  implicit def monoid[A]: Monoid[TopicPartitionValues[A]] = new Monoid[TopicPartitionValues[A]] {
    def empty = zero[A]
    def combine(x: TopicPartitionValues[A], y: TopicPartitionValues[A]) = TopicPartitionValues(x.errors ++ y.errors, x.success ++ y.success)
  }

  def from[A, B](xs: Seq[(KafkaResult, TopicPartition, A)]) = {
    val (errors, success) = xs.partition(x => x._1 != KafkaResult.NoError)

    TopicPartitionValues(errors.map(e => TopicPartitionValue(e._2, e._1)).toList, success.map(x => TopicPartitionValue(x._2, x._3)).toList)
  }
}