package vectos.kafka.akkaimpl

import java.util.{Timer, TimerTask}

import cats.data.{Kleisli, Xor, XorT}
import scodec.Attempt
import vectos.kafka.types.ir.KafkaError

import scala.concurrent.{Future, Promise}
import scala.util.Try

package object versions {

  type KafkaMonad[T] = XorT[Kleisli[Future, Kafka.Context, ?], KafkaError, T]

  protected[versions] def delay[T](delay: Long)(block: => T): Future[T] = {
    val promise = Promise[T]()
    val t = new Timer()
    t.schedule(new TimerTask {
      override def run(): Unit = {
        val _ = promise.complete(Try(block))
      }
    }, delay)
    promise.future
  }

  protected[versions] implicit class RichAttempt[A](val attempt: Attempt[A]) extends AnyVal {
    def toFuture: Future[A] = attempt match {
      case Attempt.Successful(s) => Future.successful(s)
      case Attempt.Failure(err)  => Future.failed(new Exception(err.messageWithContext))
    }

    def toXor: Xor[KafkaError, A] = attempt match {
      case Attempt.Successful(value) => Xor.right(value)
      case Attempt.Failure(err)      => Xor.left(KafkaError.CodecError(err))
    }
  }
}