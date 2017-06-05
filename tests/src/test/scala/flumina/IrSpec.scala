package flumina

import cats.Eq
import cats.instances.all._
import cats.kernel.laws.GroupLaws
import cats.laws.discipline.{ ContravariantTests, FunctorTests, InvariantTests, TraverseTests }
import cats.laws.discipline.arbitrary._
import cats.laws.discipline.eq._
import flumina._
import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest.FunSuite
import org.typelevel.discipline.scalatest.Discipline
import org.scalacheck.Shapeless._
import scodec.Attempt
import scodec.bits.ByteVector
import scodec.codecs._

class IrSpec extends FunSuite with Discipline {

  private implicit val intCodec: KafkaCodec[Int] = KafkaCodec.fromValueCodec(uint16)

  implicit def arbContravariantFunctorKafkaPartitioner[A](implicit P: KafkaPartitioner[A]): Arbitrary[KafkaPartitioner[A]] = Arbitrary(Gen.const(P))

  implicit def arbContravariantFunctorKafkaEncoder[A](implicit E: KafkaEncoder[A]): Arbitrary[KafkaEncoder[A]] = Arbitrary(Gen.const(E))

  implicit def arbCovariantFunctorKafkaDecoder[A](implicit D: KafkaDecoder[A]): Arbitrary[KafkaDecoder[A]] = Arbitrary(Gen.const(D))

  implicit def arbInvariantFunctorKafkaCodec[A](implicit C: KafkaCodec[A]): Arbitrary[KafkaCodec[A]] = Arbitrary(Gen.const(C))

  implicit val eqRecord: Eq[Record] = Eq.fromUniversalEquals[Record]

  implicit val arbByteVector: Arbitrary[ByteVector] = Arbitrary {
    for {
      ints <- Gen.listOfN(100, Gen.posNum[Int])
    } yield ByteVector(ints.map(_.toByte))
  }

  implicit def eqAttempt[A](implicit E: Eq[A]): Eq[Attempt[A]] =
    Eq.instance {
      case (Attempt.Successful(a), Attempt.Successful(b)) => E.eqv(a, b)
      case (Attempt.Failure(a), Attempt.Failure(b)) => Eq.fromUniversalEquals.eqv(a, b)
      case _ => false
    }

  implicit def eqKafkaPartitioner[A](implicit A: Arbitrary[A], E: Eq[Int]): Eq[KafkaPartitioner[A]] =
    Eq.by[KafkaPartitioner[A], (A) => Int](partitioner => (el: A) => partitioner.selectPartition(el, 10))

  implicit def eqKafkaEncoder[A](implicit A: Arbitrary[A], E: Eq[Attempt[Record]]): Eq[KafkaEncoder[A]] =
    Eq.by[KafkaEncoder[A], A => Attempt[Record]](encoder => (el: A) => encoder.encode(el))

  implicit def eqKafkaDecoder[A](implicit A: Arbitrary[Record], E: Eq[Attempt[A]]): Eq[KafkaDecoder[A]] =
    Eq.by[KafkaDecoder[A], Record => Attempt[A]](decoder => (el: Record) => decoder.decode(el))

  implicit def eqKafkaCodec[A](implicit A: Arbitrary[Record], E: Eq[Attempt[A]]): Eq[KafkaCodec[A]] =
    Eq.allEqual //TODO: how can we make this equal?

  checkAll("Contravariant[KafkaPartitioner]", ContravariantTests[KafkaPartitioner].contravariant[Int, Int, Int])
  checkAll("Contravariant[KafkaEncoder]", ContravariantTests[KafkaEncoder].contravariant[Int, Int, Int])
  checkAll("Functor[KafkaDecoder]", FunctorTests[KafkaDecoder].functor[Int, Int, Int])
  checkAll("Invariant[KafkaCodec]", InvariantTests[KafkaCodec].invariant[Int, Int, Int])
  checkAll("Monoid[TopicPartitionValues]", GroupLaws[TopicPartitionValues[Int]].monoid)
  checkAll("Traverse[TopicPartitionValues]", TraverseTests[TopicPartitionValues].traverse[Int, Int, Int, Int, Option, Option])
  checkAll("Traverse[TopicPartitionValue]", TraverseTests[TopicPartitionValue].traverse[Int, Int, Int, Int, Option, Option])
  checkAll("Traverse[OffsetValue]", TraverseTests[OffsetValue].traverse[Int, Int, Int, Int, Option, Option])
}
