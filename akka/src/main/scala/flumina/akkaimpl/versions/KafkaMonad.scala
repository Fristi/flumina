package flumina.akkaimpl.versions

import cats.data.{Kleisli, Xor, XorT}
import flumina.akkaimpl.KafkaContext
import flumina.types.ir.KafkaError

import scala.concurrent.Future

object KafkaMonad {
  def pure[A](x: A): KafkaMonad[A] =
    XorT[Kleisli[Future, KafkaContext, ?], KafkaError, A](Kleisli(_ => Future.successful(Xor.right(x))))

  def fromXor[A](x: KafkaError Xor A): KafkaMonad[A] =
    XorT[Kleisli[Future, KafkaContext, ?], KafkaError, A](Kleisli(_ => Future.successful(x)))

  def fromFutureXor[A](f: KafkaContext => Future[KafkaError Xor A]): KafkaMonad[A] =
    XorT[Kleisli[Future, KafkaContext, ?], KafkaError, A](Kleisli(f))
}