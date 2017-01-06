package flumina.monix

import flumina.akkaimpl.KafkaClient
import flumina.core.ir.{RecordEntry, TopicPartitionValue}
import monix.eval.{Callback, Task}
import monix.execution.Ack.{Continue, Stop}
import monix.execution.{Cancelable, Scheduler}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber

private[monix] class TopicConsumer(client: KafkaClient, partition: TopicPartitionValue[Long], consumptionStrategy: ConsumptionStrategy, offsetStore: OffsetStore)(implicit S: Scheduler) extends Observable[TopicPartitionValue[RecordEntry]] {

  def offsetFor(previousOffsets: TopicPartitionValue[Long])(current: TopicPartitionValue[List[RecordEntry]]) = {
    if (current.result.nonEmpty) {
      TopicPartitionValue(current.topicPartition, current.result.maxBy(_.offset).offset + 1)
    } else {
      previousOffsets
    }
  }

  def feedTask(offset: TopicPartitionValue[Long], out: Subscriber[TopicPartitionValue[RecordEntry]]): Task[Unit] = {
    Task.fromFuture(client.fetch(Set(offset))).flatMap(values =>
      if (values.errors.nonEmpty) {
        Task.now(out.onError(new TopicErrorException(values.errors)))
      } else {
        def commit = offsetStore.save(offset)
        def runNext = feedTask(offsetFor(offset)(values.success.headOption.getOrElse(TopicPartitionValue(offset.topicPartition, List.empty))), out)

        if (values.success.map(_.result.size).sum == 0) {
          consumptionStrategy match {
            case ConsumptionStrategy.TerminateEndOfStream => commit flatMap (_ => Task.now(out.onComplete()))
            case ConsumptionStrategy.Tail(delay)          => commit flatMap (_ => runNext.delayExecution(delay))
          }
        } else {
          Task.fromFuture(out.onNextAll(values.success.flatMap(x => x.result.map(y => TopicPartitionValue(x.topicPartition, y))))) flatMap {
            case Continue => commit flatMap (_ => runNext)
            case Stop     => Task.unit
          }
        }
      }
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def unsafeSubscribeFn(out: Subscriber[TopicPartitionValue[RecordEntry]]): Cancelable = {
    val callback = new Callback[Unit] {
      def onSuccess(value: Unit): Unit =
        out.onComplete()
      def onError(ex: Throwable): Unit =
        out.onError(ex)
    }

    feedTask(partition, out).runAsync(callback)
  }
}