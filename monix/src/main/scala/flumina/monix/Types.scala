package flumina.monix

import flumina.core.KafkaResult
import flumina.core.ir.TopicPartitionValue
import monix.eval.Task
import monix.execution.Ack
import scodec.Err

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

final class TopicErrorException(errors: List[TopicPartitionValue[KafkaResult]]) extends Exception

sealed trait ConsumptionStrategy

object ConsumptionStrategy {
  final case class Tail(delay: FiniteDuration) extends ConsumptionStrategy
  case object TerminateEndOfStream extends ConsumptionStrategy
}

trait CodecErrorHandler {
  def handle(err: Err): Future[Ack]
}

object CodecErrorHandler {
  val stop: CodecErrorHandler = new CodecErrorHandler {
    override def handle(err: Err): Future[Ack] = Ack.Stop
  }
}