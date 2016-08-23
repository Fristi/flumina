package flumina.types.ir

import java.nio.charset.Charset

import cats.data.Xor
import scodec.{Attempt, Err}
import flumina.types.KafkaResult

sealed trait KafkaError

object KafkaError {
  case object OtherResponseTypeExpected extends KafkaError
  final case class Error(kafkaResult: KafkaResult) extends KafkaError
  final case class MissingInfo(message: String) extends KafkaError
  final case class CodecError(err: Err) extends KafkaError
}

object KafkaList {
  def lift[T](x: Xor[KafkaError, T]): ListT[Xor[KafkaError, ?], T] =
    ListT.lift[Xor[KafkaError, ?], T](x)

  def fromAttempt[T](a: Attempt[T]): ListT[Xor[KafkaError, ?], T] = a match {
    case Attempt.Failure(err)      => ListT.lift[Xor[KafkaError, ?], T](Xor.left(KafkaError.CodecError(err)))
    case Attempt.Successful(value) => ListT.lift[Xor[KafkaError, ?], T](Xor.right(value))
  }

  def fromList[T](xs: Seq[T]): ListT[Xor[KafkaError, ?], T] =
    ListT.hoist[Xor[KafkaError, ?], T](xs.toList)

  def fromOption[T](option: Option[T], orElse: => KafkaError): ListT[Xor[KafkaError, ?], T] =
    ListT.fromOption[Xor[KafkaError, ?], T](option, Xor.left(orElse))

  def filter[T](ls: Seq[T])(f: T => Xor[KafkaError, T]): ListT[Xor[KafkaError, ?], T] =
    ListT.filter[Xor[KafkaError, ?], T](ls.toList)(f)
}

final case class Record(key: Seq[Byte], value: Seq[Byte])

object Record {
  def fromByteValue(value: Seq[Byte]): Record =
    Record(Seq.empty, value)

  def fromStringKeyValue(key: String, value: String, charSet: Charset): Record =
    Record(key.getBytes(charSet), value.getBytes(charSet))

  def fromStringValue(value: String, charSet: Charset): Record =
    Record(Seq.empty, value.getBytes(charSet))

  def fromUtf8StringKeyValue(key: String, value: String): Record =
    Record.fromStringKeyValue(key, value, Charset.forName("utf8"))

  def fromUtf8StringValue(value: String): Record =
    Record.fromStringValue(value, Charset.forName("utf8"))
}

final case class RecordEntry(offset: Long, record: Record)

final case class TopicPartition(topic: String, partition: Int)

object TopicPartition {
  def enumerate(name: String, nrPartitions: Int) = (0 until nrPartitions).map(x => TopicPartition(name, x)).toSet
}

final case class TopicPartitionResult[T](topicPartition: TopicPartition, kafkaResult: KafkaResult, value: T)

final case class GroupInfo(groupId: String, protocolType: String)

final case class OffsetMetadata(offset: Long, metadata: Option[String])

final case class Broker(nodeId: Int, host: String, port: Int)

final case class TopicInfo(leader: Int, replicas: Seq[Int], isr: Seq[Int])

final case class Metadata(brokers: Seq[Broker], metadata: Seq[TopicPartitionResult[TopicInfo]])

final case class GroupMember(
  memberId:         String,
  clientId:         Option[String],
  clientHost:       Option[String],
  consumerProtocol: Option[ConsumerProtocol],
  assignment:       Option[MemberAssignment]
)

final case class GroupProtocol(protocolName: String, consumerProtocol: Seq[ConsumerProtocol])

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

final case class MemberAssignment(version: Int, topicPartitions: Seq[TopicPartition], userData: Seq[Byte])

final case class ConsumerProtocol(version: Int, subscriptions: Seq[String], userData: Seq[Byte])