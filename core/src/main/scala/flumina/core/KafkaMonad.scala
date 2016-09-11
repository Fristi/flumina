package flumina.core

import cats.data.{Kleisli, Xor, XorT}
import flumina.core.ir.KafkaContext

object KafkaReader {
  def pure[F[_], A](x: A)(implicit F: Async[F]): KafkaReader[F, A] =
    Kleisli[F, KafkaContext, A](_ => F.pure(x))

  def fromAsync[F[_], A](f: KafkaContext => F[A])(implicit F: Async[F]): KafkaReader[F, A] =
    Kleisli(f)
}

object KafkaFailure {
  def fromAsyncXor[F[_], A](f: F[KafkaResult Xor A])(implicit F: Async[F]): KafkaFailure[F, A] =
    XorT(f)

  def fromAsync[F[_], A](f: F[A])(implicit F: Async[F]): KafkaFailure[F, A] =
    XorT(F.map(f)(Xor.right))

}