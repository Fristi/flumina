package vectos.kafka.akkaimpl

import cats.Monad
import cats.data.{Kleisli, Xor, XorT}
import scodec.Attempt
import vectos.kafka.types.ir.KafkaError

import scala.concurrent.Future

package object versions {

  type KafkaMonad[T] = XorT[Kleisli[Future, Kafka.Context, ?], KafkaError, T]

  implicit class RichXorT[B](val xorT: XorT[Kleisli[Future, Kafka.Context, ?], KafkaError, B]) extends AnyVal {
    @inline
    def mapXor[C](f: B => Xor[KafkaError, C])(implicit M: Monad[Kleisli[Future, Kafka.Context, ?]]): XorT[Kleisli[Future, Kafka.Context, ?], KafkaError, C] =
      xorT.flatMap(b => XorT.fromXor[Kleisli[Future, Kafka.Context, ?]].apply(f(b)))
  }

  implicit class RichAttempt[A](val attempt: Attempt[A]) extends AnyVal {
    def toFuture: Future[A] = attempt match {
      case Attempt.Successful(s) => Future.successful(s)
      case Attempt.Failure(err)  => Future.failed(new Exception(err.messageWithContext))
    }
  }
}
