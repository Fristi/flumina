package flumina.akkaimpl

import cats.data.{Kleisli, Xor, XorT}
import scodec.Attempt
import flumina.types.ir.KafkaError

import scala.concurrent.Future

package object versions {

  type KafkaMonad[T] = XorT[Kleisli[Future, KafkaContext, ?], KafkaError, T]

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