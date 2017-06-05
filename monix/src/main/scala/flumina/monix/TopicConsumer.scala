package flumina.monix

import cats._
import cats.implicits._
import flumina._
import flumina.client.KafkaClient
import _root_.monix.eval.{Callback, Task}
import _root_.monix.execution.Ack.{Continue, Stop}
import _root_.monix.execution.{Cancelable, Scheduler}
import _root_.monix.reactive.Observable
import _root_.monix.reactive.observers.Subscriber
import scodec.Attempt

private[monix] class TopicConsumer[A](client: KafkaClient, topicPartition: TopicPartition, initialOffset: Long, consumptionStrategy: ConsumptionStrategy, codecErrorHandler: CodecErrorHandler)(implicit S: Scheduler, D: KafkaDecoder[A]) extends Observable[TopicPartitionValue[OffsetValue[A]]] {

  private def maxByOpt[B](seq: Seq[B])(f: B => Long): Option[Long] = {
    seq.foldLeft(Option.empty[Long]) {
      case (acc, e) =>
        acc match {
          case Some(offset) if f(e) > offset => Some(f(e))
          case None                          => Some(f(e))
          case _                             => acc
        }
    }
  }

  def feedTask(offset: Long, out: Subscriber[TopicPartitionValue[OffsetValue[A]]]): Task[Unit] = {
    Task.fromFuture(client.fetch(Set(TopicPartitionValue(topicPartition, offset)))).flatMap(values =>
      if (values.errors.nonEmpty) {
        Task.now(out.onError(new TopicErrorException(values.errors)))
      } else {
        def runNext = feedTask(maxByOpt(values.success)(_.result.offset).map(_ + 1l).getOrElse(0l), out)
        val nrResults = values.success.map(_.result.size).sum

        if (nrResults == 0) {
          consumptionStrategy match {
            case ConsumptionStrategy.TerminateEndOfStream => Task.now(out.onComplete())
            case ConsumptionStrategy.Tail(delay)          => runNext.delayExecution(delay)
          }
        } else {

          val traverse = Traverse[TopicPartitionValues].compose(Traverse[OffsetValue])
          val result: Attempt[TopicPartitionValues[OffsetValue[A]]] = traverse
            .sequence[Attempt, A](traverse.map(values)(D.decode))

          result match {
            case Attempt.Successful(v) =>
              Task.fromFuture(out.onNextAll(v.success)) flatMap {
                case Continue => runNext
                case Stop     => Task.unit
              }
            case Attempt.Failure(err) =>
              Task.fromFuture(codecErrorHandler.handle(err)) flatMap {
                case Continue => runNext
                case Stop     => Task.now(out.onError(new Throwable(err.message)))
              }
          }
        }
      }
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def unsafeSubscribeFn(out: Subscriber[TopicPartitionValue[OffsetValue[A]]]): Cancelable = {
    val callback = new Callback[Unit] {
      def onSuccess(value: Unit): Unit =
        out.onComplete()

      def onError(ex: Throwable): Unit =
        out.onError(ex)
    }

    feedTask(initialOffset, out).runAsync(callback)
  }
}