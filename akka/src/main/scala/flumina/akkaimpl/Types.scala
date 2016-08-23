package flumina.akkaimpl

import akka.actor.ActorRef
import akka.util.Timeout
import cats.kernel.Monoid
import scodec.bits.BitVector
import flumina.types.ir.{OffsetMetadata, Record, TopicPartition, TopicPartitionResult}

import scala.concurrent.duration.FiniteDuration

final case class KafkaSettings(
  bootstrapBrokers:     Seq[KafkaBroker.Node],
  connectionsPerBroker: Int,
  operationalSettings:  KafkaOperationalSettings,
  requestTimeout:       FiniteDuration
)

final case class KafkaOperationalSettings(
  retryBackoff:        FiniteDuration,
  retryMaxCount:       Int,
  fetchMaxWaitTime:    FiniteDuration,
  fetchMaxBytes:       Int,
  produceTimeout:      FiniteDuration,
  groupSessionTimeout: FiniteDuration
)

final case class KafkaContext(
  connectionPool: ActorRef,
  broker:         KafkaBroker,
  requestTimeout: Timeout,
  settings:       KafkaOperationalSettings
)

sealed trait KafkaBroker

object KafkaBroker {
  final case class Node(host: String, port: Int) extends KafkaBroker
  final case object AnyNode extends KafkaBroker
}

final case class KafkaConnectionRequest(apiKey: Int, version: Int, requestPayload: BitVector, trace: Boolean)

final case class KafkaBrokerRequest(broker: KafkaBroker, request: KafkaConnectionRequest) {
  def matchesBroker(other: KafkaBroker) = broker match {
    case KafkaBroker.AnyNode           => true
    case n: KafkaBroker if n === other => true
    case _                             => false
  }
}

final case class OffsetsFetch(groupId: String, values: Set[TopicPartition])

final case class OffsetsCommit(groupId: String, offsets: Map[TopicPartition, OffsetMetadata])

final case class Produce(values: Map[TopicPartition, List[Record]])

final case class Fetch(values: Map[TopicPartition, Long])

final case class Result[A](success: Set[TopicPartitionResult[A]], errors: Set[TopicPartition])

object Result {

  def zero[A] = Result[A](Set.empty, Set.empty)

  implicit def monoid[A]: Monoid[Result[A]] = new Monoid[Result[A]] {
    def empty = zero[A]
    def combine(x: Result[A], y: Result[A]) = Result(x.success ++ y.success, x.errors ++ y.errors)
  }
}

trait HasSize[A] {
  def size(fa: A): Int
  def isEmpty(fa: A) = size(fa) === 0
  def nonEmpty(fa: A) = !isEmpty(fa)
}

object HasSize {
  implicit def setHasSize[V]: HasSize[Set[V]] = new HasSize[Set[V]] {
    override def size(fa: Set[V]) = fa.size
  }

  implicit def mapHasSize[K, V]: HasSize[Map[K, V]] = new HasSize[Map[K, V]] {
    override def size(fa: Map[K, V]) = fa.size
  }
}