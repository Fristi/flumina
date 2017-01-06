package flumina.monix

import monix.eval.Callback
import monix.execution.atomic.AtomicBoolean
import monix.execution.cancelables.{AssignableCancelable, RefCountCancelable}
import monix.execution.{Ack, Cancelable, Scheduler}
import monix.reactive._
import monix.reactive.exceptions.CompositeException
import monix.reactive.observers.Subscriber
import monix.reactive.subjects.PublishSubject

import scala.collection.mutable
import scala.concurrent.Future
import scala.math.Integral

final class PartitionConsumer[A, N](nrPartitions: Int, overflowStrategy: OverflowStrategy.Synchronous[A], partition: A => N, consumer: (Int, Seq[A]) => Future[Ack])(implicit N: Integral[N]) extends Consumer[A, Unit] {
  override def createSubscriber(cb: Callback[Unit], s: Scheduler): (Subscriber[A], AssignableCancelable) = {

    @SuppressWarnings(Array("org.wartremover.warts.MutableDataStructures"))
    val errors = mutable.ArrayBuffer.empty[Throwable]

    val obs = PublishSubject[A]()
    val pub = obs.whileBusyBuffer(overflowStrategy)
    val isUpstreamDone = AtomicBoolean(false)
    val nrPartitionsN = N.fromInt(nrPartitions)

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
        .filter(x => N.rem(partition(x), nrPartitionsN) == part)
        .bufferIntrospective(s.executionModel.recommendedBatchSize)
        .subscribe(consumerSubscriber(refCount.acquire()))
    }

    val out = new Subscriber[A] {
      implicit def scheduler: Scheduler = s

      def onNext(elem: A) = obs.onNext(elem)
      def onError(ex: Throwable) = obs.onError(ex)
      def onComplete() = obs.onComplete()
    }

    (0 until nrPartitions).foreach(subscriber)

    out -> AssignableCancelable.dummy
  }
}

object PartitionConsumer {

  def partitionConsume[A, N: Integral](nrPartitions: Int, overflowStrategy: OverflowStrategy.Synchronous[A], partition: A => N)(consumer: (Int, Seq[A]) => Future[Ack]): Consumer[A, Unit] =
    new PartitionConsumer(nrPartitions, overflowStrategy, partition, consumer)
}