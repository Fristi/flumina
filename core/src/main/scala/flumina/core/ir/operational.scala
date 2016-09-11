package flumina.core.ir

import scodec.bits.{BitVector, ByteVector}

import scala.concurrent.duration._

final case class KafkaSettings(
  bootstrapBrokers:     Seq[KafkaBroker.Node],
  connectionsPerBroker: Int,
  operationalSettings:  KafkaOperationalSettings,
  requestTimeout:       FiniteDuration
)

final case class KafkaOperationalSettings(
    retryBackoff:              FiniteDuration,
    retryMaxCount:             Int,
    fetchMaxWaitTime:          FiniteDuration,
    fetchMaxBytes:             Int,
    produceTimeout:            FiniteDuration,
    groupSessionTimeout:       FiniteDuration,
    heartbeatFrequency:        Int,
    consumeAssignmentStrategy: ConsumeAssignmentStrategy
) {

  lazy val heartbeatInterval = (groupSessionTimeout.toMillis / heartbeatFrequency).milliseconds
}

sealed trait KafkaBroker

object KafkaBroker {
  final case class Node(host: String, port: Int) extends KafkaBroker
  final case object AnyNode extends KafkaBroker
}

final case class KafkaConnectionRequest(apiKey: Int, version: Int, requestPayload: BitVector, trace: Boolean)

final case class KafkaBrokerRequest(broker: KafkaBroker, request: KafkaConnectionRequest) {
  def matchesBroker(other: KafkaBroker) = broker match {
    case KafkaBroker.AnyNode               => true
    case n: KafkaBroker if n.equals(other) => true
    case _                                 => false
  }
}

final case class KafkaContext(
  broker:   KafkaBroker,
  settings: KafkaOperationalSettings
)

trait ConsumeAssignmentStrategy {
  def assign(leaderId: String, topicPartitions: Set[TopicPartition], members: Seq[GroupMember]): Seq[GroupAssignment]
}

object ConsumeAssignmentStrategy {
  lazy val allToLeader = new ConsumeAssignmentStrategy {
    override def assign(leaderId: String, topicPartitions: Set[TopicPartition], members: Seq[GroupMember]): Seq[GroupAssignment] =
      Seq(GroupAssignment(leaderId, MemberAssignment(0, topicPartitions.toSeq, ByteVector.empty)))
  }

  //  lazy val evenly = new ConsumeAssignmentStrategy {
  //    override def assign(leaderId: String, topicPartitions: Set[TopicPartition], members: Seq[GroupMember]): Seq[GroupAssignment] = {
  //      def splitEvenly[A](sequence: TraversableOnce[A], over: Int) = {
  //        @tailrec
  //        def go(seq: List[A], acc: List[List[A]], split: Int): List[List[A]] =
  //          if (seq.size <= split) seq :: acc
  //          else go(seq.drop(split), seq.take(split) :: acc, split)
  //
  //        go(sequence.toList, List.empty, sequence.size / over)
  //      }
  //
  //      val topicPartitionChunks = splitEvenly(topicPartitions, members.size)
  //
  //      members
  //        .zipWithIndex
  //        .map { case (m, idx) => GroupAssignment(m.memberId, MemberAssignment(0, topicPartitionChunks(idx), Nil)) }
  //    }
  //  }
}
