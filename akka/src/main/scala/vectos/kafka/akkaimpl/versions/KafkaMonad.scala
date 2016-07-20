package vectos.kafka.akkaimpl.versions

import cats.data.{Kleisli, Xor, XorT}
import vectos.kafka.akkaimpl.Kafka
import vectos.kafka.types.ir.KafkaError

import scala.concurrent.Future

object KafkaMonad {
  def fromXor[A](x: KafkaError Xor A): KafkaMonad[A] =
    XorT[Kleisli[Future, Kafka.Context, ?], KafkaError, A](Kleisli(_ => Future.successful(x)))

  def fromFutureXor[A](f: Kafka.Context => Future[KafkaError Xor A]): KafkaMonad[A] =
    XorT[Kleisli[Future, Kafka.Context, ?], KafkaError, A](Kleisli(f))
}