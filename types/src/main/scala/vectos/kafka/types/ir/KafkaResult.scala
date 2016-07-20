package vectos.kafka.types.ir

import cats.data.Xor
import scodec.{Attempt, Err}
import vectos.kafka.types.KafkaResult

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

final case class MessageEntry(offset: Long, key: Seq[Byte], value: Seq[Byte])

final case class TopicPartition(topic: String, partition: Int)

final case class TopicPartitionResult[T](topicPartition: TopicPartition, kafkaResult: KafkaResult, value: T)

final case class GroupInfo(groupId: String, protocolType: String)

final case class OffsetMetadata(offset: Long, metadata: Option[String])

final case class Broker(nodeId: Int, host: String, port: Int)

final case class TopicInfo(leader: Int, replicas: Seq[Int], isr: Seq[Int])

final case class Metadata(brokers: Seq[Broker], metadata: Seq[TopicPartitionResult[TopicInfo]])

final case class GroupMember(
  memberId:                 String,
  clientId:                 Option[String],
  clientHost:               Option[String],
  consumerProtocolMetadata: Option[ConsumerProtocolMetadata],
  assignment:               Option[MemberAssignment]
)

final case class GroupProtocol(protocolName: String, protocolMetadata: Seq[ConsumerProtocolMetadata])

final case class JoinGroupResult(generationId: Int, groupProtocol: String, leaderId: String, memberId: String, members: Seq[GroupMember])

final case class GroupCoordinator(coordinatorId: Int, coordinatorHost: String, coordinatorPort: Int)

final case class Group(
  errorCode:    KafkaResult,
  groupId:      String,
  state:        String,
  protocolType: String,
  protocol:     String,
  members:      Seq[GroupMember]
)

final case class MemberAssignmentTopicPartition(topicName: String, partitions: Seq[Int])

final case class GroupAssignment(memberId: String, memberAssignment: MemberAssignment)

final case class MemberAssignment(version: Int, topicPartition: Seq[MemberAssignmentTopicPartition], userData: Seq[Byte])

final case class ConsumerProtocolMetadata(version: Int, subscriptions: Seq[String], userData: Seq[Byte])