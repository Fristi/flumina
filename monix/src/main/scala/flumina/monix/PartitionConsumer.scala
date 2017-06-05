package flumina.monix

import java.util.concurrent.atomic.AtomicInteger

import monix.eval.{Callback, Task}
import monix.execution.atomic.AtomicBoolean
import monix.execution.cancelables.{AssignableCancelable, RefCountCancelable}
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive._
import monix.execution.exceptions.CompositeException
import monix.reactive.observers.Subscriber
import monix.reactive.subjects.PublishSubject

import scala.collection.mutable
import scala.concurrent.Future

final class PartitionConsumer[A](parts: Int, overflowStrategy: OverflowStrategy.Synchronous[A], partition: (Int, A) => Int, consumer: (Int, Seq[A]) => Future[Ack]) extends Consumer[A, Unit] {
  override def createSubscriber(cb: Callback[Unit], s: Scheduler): (Subscriber[A], AssignableCancelable) = {

    @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
    val errors = mutable.ArrayBuffer.empty[Throwable]

    val publishSubject = PublishSubject[A]()
    val pub            = publishSubject.whileBusyBuffer(overflowStrategy)
    val isUpstreamDone = AtomicBoolean(false)
    val refCount = RefCountCancelable { () =>
      errors.synchronized {
        if (errors.nonEmpty) {
          cb.onError(CompositeException(errors))
        } else {
          cb.onSuccess(())
        }
      }
    }

    def subscriber(part: Int): Cancelable = {

      def consumerSubscriber(refID: Cancelable) = new Subscriber[Seq[A]] {
        implicit def scheduler: Scheduler = s

        def onNext(elem: Seq[A]) =
          consumer(part, elem)

        def onError(ex: Throwable) = errors.synchronized {
          errors += ex
          onComplete()
        }

        def onComplete() = {
          if (!isUpstreamDone.getAndSet(true)) {
            refCount.cancel()
          }

          refID.cancel()
        }
      }

      pub
        .filter(x => partition(parts, x) == part)
        .bufferIntrospective(s.executionModel.recommendedBatchSize)
        .subscribe(consumerSubscriber(refCount.acquire()))
    }

    val out = new Subscriber[A] {
      implicit def scheduler: Scheduler = s

      def onNext(elem: A): Future[Ack] = publishSubject.onNext(elem)

      def onError(ex: Throwable): Unit = publishSubject.onError(ex)

      def onComplete(): Unit = publishSubject.onComplete()
    }

    0.until(parts).foreach(subscriber)

    out -> AssignableCancelable.dummy
  }
}

object PartitionConsumer {

  def partitionConsume[A](parts: Int, overflowStrategy: OverflowStrategy.Synchronous[A], partition: (Int, A) => Int)(consumer: (Int, Seq[A]) => Future[Ack]): Consumer[A, Unit] =
    new PartitionConsumer(parts, overflowStrategy, partition, consumer)
}
