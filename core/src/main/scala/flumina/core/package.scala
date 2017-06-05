package flumina

import scodec.bits.ByteVector
import scodec.codecs._
import scodec.{Attempt, Codec, Err}

package object core {

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
    def updatedValue(key: K, default: => V)(update: V => V): Map[K, V] =
      map.updated(key, update(map.getOrElse(key, default)))
  }

  val kafkaOptionalString: Codec[Option[String]] = new KafkaStringCodec
  val kafkaRequiredString: Codec[String] = {
    def encode(s: String): Attempt[Option[String]] = Attempt.Successful(Some(s))
    def decode(strOpt: Option[String]): Attempt[String] = strOpt match {
      case None    => Attempt.failure(Err("String required but got null"))
      case Some(s) => Attempt.successful(s)
    }
    kafkaOptionalString.exmap(decode, encode)
  }

  def kafkaBool: Codec[Boolean] = int8.xmap[Boolean](x => x == 1, x => if (x) 1 else 0)

  val kafkaBytes: Codec[ByteVector] = new KafkaBytesCodec

  def kafkaNullableArray[A](valueCodec: Codec[A]): Codec[Vector[A]] =
    new KafkaNullableArrayCodec(valueCodec)

  def kafkaArray[A](valueCodec: Codec[A]): Codec[Vector[A]] =
    vectorOfN(int32, valueCodec)

  def kafkaMap[K, V](keyCodec: Codec[K], valueCodec: Codec[V]): Codec[Map[K, V]] =
    kafkaArray(keyCodec ~ valueCodec).xmap(_.toMap, _.toVector)

  val kafkaOptionalInt32: Codec[Option[Int]] = {
    def decode(x: Int): Option[Int] = if (x === -1) None else Some(x)
    def encode(x: Option[Int]) = x match {
      case Some(n) => n
      case None    => -1
    }

    int32.xmap(decode, encode)
  }

  val kafkaOptionalInt16: Codec[Option[Int]] = {
    def decode(x: Int): Option[Int] = if (x === -1) None else Some(x)
    def encode(x: Option[Int]) = x match {
      case Some(n) => n
      case None    => -1
    }

    int16.xmap(decode, encode)
  }
}
