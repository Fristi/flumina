package vectos.kafka.akkaimpl

import scala.concurrent.duration._

trait KafkaConnectionRetryStrategy {
  def nextDelay(times: Int): Option[FiniteDuration]
}

object KafkaConnectionRetryStrategy {

  final class Infinite(delay: FiniteDuration) extends KafkaConnectionRetryStrategy {
    def nextDelay(times: Int) = Some(delay)
  }
}
