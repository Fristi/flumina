package flumina.core

import cats.Monad
import scodec.Attempt

trait Async[F[_]] extends Monad[F] {
  def fail[A](err: Throwable): F[A]
}

object Async {
  def fromAttempt[F[_], A](attempt: Attempt[A])(implicit F: Async[F]): F[A] = attempt match {
    case Attempt.Successful(s) => F.pure(s)
    case Attempt.Failure(err)  => F.fail(new Exception(s"Fail: ${err.messageWithContext}"))
  }
}