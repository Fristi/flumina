import cats._
import cats.implicits._

import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Attempt, Codec, Err}

package object flumina {

  private[flumina] def attempt[A](a: => A): Attempt[A] = {
    try { Attempt.successful(a) } catch {
      case t: Throwable => Attempt.failure(Err(s"${t.getClass} : ${t.getMessage}"))
    }
  }

  private[flumina] implicit val eqBitVector: Eq[BitVector] = Eq.fromUniversalEquals[BitVector]

  private[flumina] implicit class RichMap[K, V](val map: Map[K, V]) {

    @inline
    def updatedValue(key: K, default: => V)(update: V => V): Map[K, V] =
      map.updated(key, update(map.getOrElse(key, default)))
  }

  private[flumina] val kafkaOptionalString: Codec[Option[String]] = new KafkaStringCodec
  private[flumina] val kafkaRequiredString: Codec[String] = {
    def encode(s: String): Attempt[Option[String]] = Attempt.Successful(Some(s))
    def decode(strOpt: Option[String]): Attempt[String] = strOpt match {
      case None    => Attempt.failure(Err("String required but got null"))
      case Some(s) => Attempt.successful(s)
    }
    kafkaOptionalString.exmap(decode, encode)
  }

  private[flumina] def kafkaBool: Codec[Boolean] =
    int8.xmap[Boolean](x => x == 1, x => if (x) 1 else 0)

  private[flumina] val kafkaBytes: Codec[ByteVector] = new KafkaBytesCodec

  private[flumina] def kafkaNullableArray[A](valueCodec: Codec[A]): Codec[Vector[A]] =
    new KafkaNullableArrayCodec(valueCodec)

  private[flumina] def kafkaArray[A](valueCodec: Codec[A]): Codec[Vector[A]] =
    vectorOfN(int32, valueCodec)

  private[flumina] def kafkaMap[K, V](keyCodec: Codec[K], valueCodec: Codec[V]): Codec[Map[K, V]] =
    kafkaArray(keyCodec ~ valueCodec).xmap(_.toMap, _.toVector)

  private[flumina] val kafkaOptionalInt32: Codec[Option[Int]] = {
    def decode(x: Int): Option[Int] = if (x === -1) None else Some(x)
    def encode(x: Option[Int]) = x match {
      case Some(n) => n
      case None    => -1
    }

    int32.xmap(decode, encode)
  }

  private[flumina] val kafkaOptionalInt16: Codec[Option[Int]] = {
    def decode(x: Int): Option[Int] = if (x === -1) None else Some(x)
    def encode(x: Option[Int]) = x match {
      case Some(n) => n
      case None    => -1
    }

    int16.xmap(decode, encode)
  }
}
