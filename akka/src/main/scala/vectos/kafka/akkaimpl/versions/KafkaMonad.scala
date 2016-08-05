package vectos.kafka.akkaimpl.versions

import cats.data.{Kleisli, Xor, XorT}
import vectos.kafka.akkaimpl.KafkaContext
import vectos.kafka.types.ir.KafkaError

import scala.concurrent.Future

object KafkaMonad {
  def pure[A](x: A): KafkaMonad[A] =
    XorT[Kleisli[Future, KafkaContext, ?], KafkaError, A](Kleisli(_ => Future.successful(Xor.right(x))))

  def fromXor[A](x: KafkaError Xor A): KafkaMonad[A] =
    XorT[Kleisli[Future, KafkaContext, ?], KafkaError, A](Kleisli(_ => Future.successful(x)))

  def fromFutureXor[A](f: KafkaContext => Future[KafkaError Xor A]): KafkaMonad[A] =
    XorT[Kleisli[Future, KafkaContext, ?], KafkaError, A](Kleisli(f))
}