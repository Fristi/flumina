package flumina.monix

import flumina.core.KafkaResult
import flumina.core.ir.TopicPartitionValue
import monix.eval.Task

import scala.concurrent.duration.FiniteDuration

final class TopicErrorException(errors: List[TopicPartitionValue[KafkaResult]]) extends Exception

trait OffsetStore {
  def load(topic: String): Task[Vector[TopicPartitionValue[Long]]]
  def save(offset: TopicPartitionValue[Long]): Task[Unit]
}

sealed trait ConsumptionStrategy

object ConsumptionStrategy {
  final case class Tail(delay: FiniteDuration) extends ConsumptionStrategy
  case object TerminateEndOfStream extends ConsumptionStrategy
}