package flumina.monix

import cats._
import cats.implicits._
import cats.free.Free
import flumina.core.ir.{TopicPartition, TopicPartitionValue}
import flumina.monix.KeyValueStore.{Atomically, FromTry, Get, MultiGet, Pure, Put}
import monix.eval.{Task, TaskSemaphore}
import monix.cats._
import org.rocksdb.{Options, RocksDB}
import scodec._
import scodec.bits.BitVector
import scodec.codecs._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

sealed trait KeyValueStore[A]

final class RocksDbInterpreter(dir: String) extends (KeyValueStore ~> Task) { self =>
  private val options = new Options()
  private val o2 = options.setCreateIfMissing(true)
  private val rocksDB = RocksDB.open(o2, dir)
  private val semaphore = TaskSemaphore(maxParallelism = 1)

  override def apply[A](fa: KeyValueStore[A]) = fa match {
    case Pure(prg) =>
      Task.pure(prg)

    case FromTry(prg) =>
      Task.fromTry(prg)

    case Get(key) =>
      Task.fromTry(Try(Option(rocksDB.get(key.toByteArray)).map(BitVector.view)))

    case MultiGet(keys) =>
      Task.fromTry(Try(rocksDB.multiGet(keys.map(_.toByteArray).asJava)))
        .map(_.asScala.toList.map { case (k, v) => BitVector.view(k) -> BitVector.view(v) })

    case Put(key, value) =>
      Task.fromTry(Try(rocksDB.put(key.toByteArray, value.toByteArray)))

    case Atomically(prg) => semaphore.greenLight(prg.foldMap(self))
  }
}

object KeyValueStore {

  implicit class RichAttempt[A](val attempt: Attempt[A]) {
    def toTry = attempt match {
      case Attempt.Failure(err)      => Failure(new Exception(s"Something went wrong: ${err.messageWithContext}"))
      case Attempt.Successful(value) => Success(value)
    }
  }

  final case class FromTry[A](prg: Try[A]) extends KeyValueStore[A]
  final case class Get(key: BitVector) extends KeyValueStore[Option[BitVector]]
  final case class MultiGet(keys: List[BitVector]) extends KeyValueStore[List[(BitVector, BitVector)]]
  final case class Put(key: BitVector, value: BitVector) extends KeyValueStore[Unit]
  final case class Atomically[A](prg: Free[KeyValueStore, A]) extends KeyValueStore[A]
  final case class Pure[A](prg: A) extends KeyValueStore[A]

  def fromTry[A](prg: Try[A]): Free[KeyValueStore, A] = Free.liftF(FromTry(prg))
  def fromAttempt[A](prg: Attempt[A]): Free[KeyValueStore, A] = fromTry(prg.toTry)
  def pure[A](prg: A): Free[KeyValueStore, A] = Free.liftF(Pure(prg))
  def get(key: BitVector): Free[KeyValueStore, Option[BitVector]] = Free.liftF(Get(key))
  def multiget(keys: List[BitVector]): Free[KeyValueStore, List[(BitVector, BitVector)]] = Free.liftF(MultiGet(keys))
  def put(key: BitVector, value: BitVector): Free[KeyValueStore, Unit] = Free.liftF(Put(key, value))
  def atomically[A](prg: Free[KeyValueStore, A]): Free[KeyValueStore, A] = Free.liftF(Atomically(prg))
}

class KeyValueOffsetStore(exec: KeyValueStore ~> Task) extends OffsetStore {
  import KeyValueStore._

  def load(topic: String) = {
    val prg = atomically {
      for {
        topic <- get(BitVector(topic.getBytes))
        partitions <- topic match {
          case Some(bytes) => fromAttempt(codecs.index.decodeValue(bytes))
          case None        => pure(Vector.empty[TopicPartitionValue[Long]])
        }
      } yield partitions
    }

    prg.foldMap(exec)
  }

  def save(offset: TopicPartitionValue[Long]) = {
    val topicName = BitVector(offset.topicPartition.topic.getBytes)

    def modify(bits: BitVector) = for {
      current <- fromAttempt(codecs.index.decodeValue(bits))
      newValue <- fromAttempt(codecs.index.encode(current.filterNot(_.topicPartition == offset.topicPartition) :+ offset))
      _ <- put(topicName, newValue)
    } yield ()

    val prg = atomically {
      for {
        topic <- get(topicName)
        _ <- topic match {
          case Some(bits) => modify(bits)
          case None       => fromAttempt(codecs.index.encode(Vector(offset))) flatMap (value => put(topicName, value))
        }
      } yield ()
    }

    prg.foldMap(exec)
  }

  private object codecs {

    def topicPartitionValue[A](valueCodec: Codec[A]) =
      (("topicPartition" | topicPartition) :: ("value" | valueCodec)).as[TopicPartitionValue[A]]

    val topicPartition =
      (("topic" | variableSizeBytes(uint8, ascii)) :: ("partition" | uint16)).as[TopicPartition]

    val index = vectorOfN(uint8, topicPartitionValue(uint32))
  }
}

final class RocksDbOffsetStore(dir: String) extends KeyValueOffsetStore(new RocksDbInterpreter(dir))
