package flumina

import cats.Semigroup
import cats.data.{Ior, Kleisli, XorT}
import flumina.core.ir.KafkaContext
import scodec.bits.ByteVector
import scodec.{Attempt, Codec, Err}
import scodec.codecs._

package object core {

  implicit def catsDataSemigroupIor[A: Semigroup, B: Semigroup]: Semigroup[Ior[A, B]] = new Semigroup[Ior[A, B]] {
    override def combine(x: Ior[A, B], y: Ior[A, B]): Ior[A, B] = x append y
  }

  def attempt[A](a: => A): Attempt[A] = {
    try { Attempt.successful(a) }
    catch { case t: Throwable => Attempt.failure(Err(s"${t.getClass} : ${t.getMessage}")) }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit final class AnyOps[A](self: A) {
    @inline
    def ===(other: A): Boolean = self == other

    @inline
    def =/=(other: A): Boolean = self != other
  }

  implicit class RichMap[K, V](val map: Map[K, V]) {

    @inline
    def updatedValue(key: K, default: => V)(update: V => V) =
      map.updated(key, update(map.getOrElse(key, default)))
  }

  type KafkaReader[F[_], T] = Kleisli[F, KafkaContext, T]
  type KafkaFailure[F[_], T] = XorT[F, KafkaResult, T]

  val kafkaOptionalString: Codec[Option[String]] = new KafkaStringCodec
  val kafkaRequiredString: Codec[String] = {
    def encode(s: String): Attempt[Option[String]] = Attempt.Successful(Some(s))
    def decode(strOpt: Option[String]): Attempt[String] = strOpt match {
      case None    => Attempt.failure(Err("String required but got null"))
      case Some(s) => Attempt.successful(s)
    }
    kafkaOptionalString.exmap(decode, encode)
  }

  val kafkaBytes: Codec[ByteVector] = new KafkaBytesCodec

  def kafkaArray[A](valueCodec: Codec[A]): Codec[Vector[A]] = vectorOfN(int32, valueCodec)
}
