package flumina

import java.util.UUID

import cats.functor.{Contravariant, Invariant}
import cats.implicits._
import cats.{Applicative, Eq, Eval, Functor, Monoid, Traverse}
import scodec.bits.ByteVector
import scodec.{Attempt, Codec, Decoder, Encoder, Err}

abstract class Compression(val id: Int)

object Compression {
  def apply(id: Int): Attempt[Compression] = id match {
    case 1 => Attempt.successful(GZIP)
    case 2 => Attempt.successful(Snappy)
    case _ => Attempt.failure(Err("Compression not recognized"))
  }

  case object GZIP   extends Compression(1)
  case object Snappy extends Compression(2)
}

abstract class MessageVersion(val id: Int)

object MessageVersion {
  case object V0 extends MessageVersion(0)
  case object V1 extends MessageVersion(1)
}

final case class ReplicationAssignment(partitionId: Int, replicas: Seq[Int])

final case class TopicDescriptor(
    topic: String,
    nrPartitions: Option[Int],
    replicationFactor: Option[Int],
    replicaAssignment: Seq[ReplicationAssignment],
    config: Map[String, String]
)

final case class Record(key: ByteVector, value: ByteVector)

final case class OffsetValue[A](offset: Long, record: A)

object OffsetValue {

  implicit val traverseOffsetValue: Traverse[OffsetValue] = new Traverse[OffsetValue] {
    override def traverse[G[_], A, B](fa: OffsetValue[A])(f: (A) => G[B])(implicit A: Applicative[G]): G[OffsetValue[B]] =
      A.map(f(fa.record))(b => OffsetValue(fa.offset, b))

    override def foldLeft[A, B](fa: OffsetValue[A], b: B)(f: (B, A) => B): B = f(b, fa.record)

    override def foldRight[A, B](fa: OffsetValue[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = f(fa.record, lb)
  }

  implicit def eqOffsetValue[A](implicit E: Eq[A]): Eq[OffsetValue[A]] = new Eq[OffsetValue[A]] {
    override def eqv(x: OffsetValue[A], y: OffsetValue[A]): Boolean =
      x.offset === y.offset && E.eqv(x.record, y.record)
  }
}

final case class TopicPartition(topic: String, partition: Int)

object TopicPartition {
  def enumerate(name: String, nrPartitions: Int): Set[TopicPartition] =
    (0 until nrPartitions).map(x => TopicPartition(name, x)).toSet

  implicit val eqTopicPartition: Eq[TopicPartition] = new Eq[TopicPartition] {
    override def eqv(x: TopicPartition, y: TopicPartition): Boolean =
      x.topic === y.topic && x.partition === y.partition
  }
}

final case class ApiVersion(apiKey: Int, minVersion: Int, maxVersion: Int)

final case class GroupInfo(groupId: String, protocolType: String)

final case class OffsetMetadata(offset: Long, metadata: Option[String])

final case class Broker(nodeId: Int, host: String, port: Int)

final case class TopicInfo(leader: Int, replicas: Seq[Int], isr: Seq[Int])

final case class TopicResult(topic: String, kafkaResult: KafkaResult)

final case class Metadata(
    brokers: Set[Broker],
    controller: Broker,
    topics: Set[TopicPartitionValue[TopicInfo]],
    topicsInError: Set[TopicResult]
)

final case class GroupMember(
    memberId: String,
    clientId: Option[String],
    clientHost: Option[String],
    consumerProtocol: Option[ConsumerProtocol],
    assignment: Option[MemberAssignment]
)

final case class GroupProtocol(protocolName: String, consumerProtocols: Seq[ConsumerProtocol])

final case class JoinGroupResult(generationId: Int, groupProtocol: String, leaderId: String, memberId: String, members: Seq[GroupMember])

final case class Group(
    kafkaResult: KafkaResult,
    groupId: String,
    state: String,
    protocolType: String,
    protocol: String,
    members: Seq[GroupMember]
)

final case class GroupAssignment(memberId: String, memberAssignment: MemberAssignment)

final case class MemberAssignment(version: Int, topicPartitions: Seq[TopicPartition], userData: ByteVector)

final case class ConsumerProtocol(version: Int, subscriptions: Seq[String], userData: ByteVector)

final case class TopicPartitionValue[T](topicPartition: TopicPartition, result: T)

object TopicPartitionValue {
  implicit def eqTopicPartitionValue[A](implicit E: Eq[A]): Eq[TopicPartitionValue[A]] =
    new Eq[TopicPartitionValue[A]] {
      override def eqv(x: TopicPartitionValue[A], y: TopicPartitionValue[A]): Boolean =
        Eq[TopicPartition].eqv(x.topicPartition, y.topicPartition) && E.eqv(x.result, y.result)
    }

  implicit val traverseTopicPartitionValue: Traverse[TopicPartitionValue] =
    new Traverse[TopicPartitionValue] {
      override def traverse[G[_], A, B](fa: TopicPartitionValue[A])(f: (A) => G[B])(implicit A: Applicative[G]): G[TopicPartitionValue[B]] =
        A.map(f(fa.result))(y => TopicPartitionValue(fa.topicPartition, y))

      override def foldLeft[A, B](fa: TopicPartitionValue[A], b: B)(f: (B, A) => B): B =
        f(b, fa.result)

      override def foldRight[A, B](fa: TopicPartitionValue[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = f(fa.result, lb)
    }
}

final case class TopicPartitionValues[T](errors: List[TopicPartitionValue[KafkaResult]], success: List[TopicPartitionValue[T]]) {

  lazy val canBeRetried: Set[TopicPartition] =
    errors.filter(x => KafkaResult.canRetry(x.result)).map(_.topicPartition).toSet

  def resultsExceptWhichCanBeRetried: TopicPartitionValues[T] =
    TopicPartitionValues(errors.filterNot(x => KafkaResult.canRetry(x.result)), success)
}

object TopicPartitionValues {
  def zero[A]: TopicPartitionValues[A] =
    TopicPartitionValues(List.empty, List.empty[TopicPartitionValue[A]])

  implicit def monoidTopicPartitionValues[A]: Monoid[TopicPartitionValues[A]] =
    new Monoid[TopicPartitionValues[A]] {
      def empty: TopicPartitionValues[A] = zero[A]
      def combine(x: TopicPartitionValues[A], y: TopicPartitionValues[A]): TopicPartitionValues[A] =
        TopicPartitionValues(x.errors ++ y.errors, x.success ++ y.success)
    }

  implicit def eqTopicPartitionValues[A](implicit E: Eq[TopicPartitionValue[A]]): Eq[TopicPartitionValues[A]] =
    new Eq[TopicPartitionValues[A]] {
      override def eqv(x: TopicPartitionValues[A], y: TopicPartitionValues[A]): Boolean =
        (x.success corresponds y.success)(E.eqv)
    }

  implicit val traverseTopicPartitionValues: Traverse[TopicPartitionValues] =
    new Traverse[TopicPartitionValues] {

      override def traverse[G[_], A, B](fa: TopicPartitionValues[A])(f: (A) => G[B])(implicit A: Applicative[G]): G[TopicPartitionValues[B]] =
        A.map(fa.success.traverse(x => A.map(f(x.result))(y => TopicPartitionValue(x.topicPartition, y))))(res => TopicPartitionValues(fa.errors, res))

      override def foldLeft[A, B](fa: TopicPartitionValues[A], b: B)(f: (B, A) => B): B =
        fa.success.foldLeft(b) { case (acc, e) => f(acc, e.result) }

      override def foldRight[A, B](fa: TopicPartitionValues[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = Traverse[List].foldRight(fa.success, lb) {
        case (acc, e) => f(acc.result, e)
      }
    }

  def from[A, B](xs: Seq[(KafkaResult, TopicPartition, A)]): TopicPartitionValues[A] = {
    val (errors, success) = xs.partition(x => x._1 != KafkaResult.NoError)

    TopicPartitionValues(
      errors.map(e => TopicPartitionValue(e._2, e._1)).toList,
      success.map(x => TopicPartitionValue(x._2, x._3)).toList
    )
  }
}

trait KafkaPartitioner[A] {
  def selectPartition(value: A, nrPartitions: Int): Int
}

object KafkaPartitioner {
  def instance[A](f: (A, Int) => Int): KafkaPartitioner[A] = new KafkaPartitioner[A] {
    override def selectPartition(value: A, nrPartitions: Int): Int = f(value, nrPartitions)
  }

  def by[B, A: KafkaPartitioner](f: B => A): KafkaPartitioner[B] =
    instance {
      case (v, nrPartitions) => implicitly[KafkaPartitioner[A]].selectPartition(f(v), nrPartitions)
    }

  implicit val longPartitioner: KafkaPartitioner[Long] = instance[Long] {
    case (v, nrPartitions) => (v % nrPartitions).toInt
  }
  implicit val intPartitioner: KafkaPartitioner[Int] = instance[Int] {
    case (v, nrPartitions) => v % nrPartitions
  }
  implicit val uuidPartition: KafkaPartitioner[UUID] = instance[UUID] {
    case (v, nrPartitions) => v.hashCode % nrPartitions
  }
  implicit val stringPartitioner: KafkaPartitioner[String] = instance[String] {
    case (v, nrPartitions) => v.hashCode % nrPartitions
  }

  implicit val contravariantKafkaPartitioner: Contravariant[KafkaPartitioner] =
    new Contravariant[KafkaPartitioner] {
      override def contramap[A, B](fa: KafkaPartitioner[A])(f: (B) => A): KafkaPartitioner[B] =
        instance[B] { case (v, nrPartitions) => fa.selectPartition(f(v), nrPartitions) }
    }
}

trait KafkaEncoder[A] {
  def encode(value: A): Attempt[Record]
}

object KafkaEncoder {

  def fromKeyValueEncoder[K, V](ke: Encoder[K], ve: Encoder[V]): KafkaEncoder[(K, V)] =
    instance[(K, V)] {
      case (k, v) =>
        for {
          kb <- ke.encode(k)
          vb <- ve.encode(v)
        } yield Record(kb.toByteVector, vb.toByteVector)
    }

  def fromValueEncoder[V](ve: Encoder[V]): KafkaEncoder[V] =
    instance[V](v => ve.encode(v).map(b => Record(ByteVector.empty, b.toByteVector)))

  def instance[A](f: A => Attempt[Record]): KafkaEncoder[A] = new KafkaEncoder[A] {
    override def encode(value: A): Attempt[Record] = f(value)
  }

  implicit val contravariant: Contravariant[KafkaEncoder] = new Contravariant[KafkaEncoder] {
    override def contramap[A, B](fa: KafkaEncoder[A])(f: (B) => A): KafkaEncoder[B] =
      instance(v => fa.encode(f(v)))
  }
}

trait KafkaDecoder[A] {
  def decode(value: Record): Attempt[A]
}

object KafkaDecoder {
  def fromKeyValueDecoder[K, V](kd: Decoder[K], vd: Decoder[V]): KafkaDecoder[(K, V)] =
    instance[(K, V)] { record =>
      for {
        k <- kd.decode(record.key.toBitVector)
        v <- vd.decode(record.value.toBitVector)
      } yield k.value -> v.value
    }

  def fromValueDecoder[V](vd: Decoder[V]): KafkaDecoder[V] =
    instance[V](r => vd.decode(r.value.toBitVector).map(_.value))

  def instance[A](f: Record => Attempt[A]): KafkaDecoder[A] = new KafkaDecoder[A] {
    override def decode(value: Record): Attempt[A] = f(value)
  }

  implicit val functor: Functor[KafkaDecoder] = new Functor[KafkaDecoder] {
    override def map[A, B](fa: KafkaDecoder[A])(f: (A) => B): KafkaDecoder[B] =
      instance(v => fa.decode(v).map(f))
  }
}

trait KafkaCodec[A] extends KafkaEncoder[A] with KafkaDecoder[A] { self =>
  def imap[B](f: A => B, g: B => A): KafkaCodec[B] = new KafkaCodec[B] {
    override def encode(value: B): Attempt[Record] = self.encode(g(value))
    override def decode(value: Record): Attempt[B] = self.decode(value).map(f)
  }
}

object KafkaCodec {
  def fromValueCodec[A](codec: Codec[A]): KafkaCodec[A] = new KafkaCodec[A] {
    override def encode(value: A): Attempt[Record] =
      codec.encode(value).map(bv => Record(ByteVector.empty, bv.toByteVector))
    override def decode(value: Record): Attempt[A] = codec.decodeValue(value.value.toBitVector)
  }

  implicit val invariantKafkaCodec: Invariant[KafkaCodec] = new Invariant[KafkaCodec] {
    override def imap[A, B](fa: KafkaCodec[A])(f: (A) => B)(g: (B) => A): KafkaCodec[B] =
      fa.imap(f, g)
  }
}
