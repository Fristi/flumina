package flumina.monix

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._

import cats.implicits._
import cats.free.Free
import monix.cats._
import cats.scalatest.EitherMatchers
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scodec.bits.BitVector

import scala.util.Success

class RocksDbSpec extends Suite with WordSpecLike with ScalaFutures
    with Matchers
    with Inspectors
    with EitherMatchers
    with EitherValues
    with OptionValues
    with BeforeAndAfterAll {

  import KeyValueStore._
  import monix.execution.Scheduler.Implicits.global

  val dir = "tests/target/tests-data"
  def keyBits(i: Int) = BitVector(s"key$i".getBytes)
  val valueBits = BitVector("value".getBytes)
  val rocksDb = new RocksDbInterpreter(dir)

  def exec[A](prg: Free[KeyValueStore, A]) = prg.foldMap(rocksDb).runAsync

  "RocksDb" should {
    "get" in {
      whenReady(exec(get(keyBits(0))))(_ shouldBe None)
    }

    "put" in {
      val prg = for {
        _ <- put(keyBits(0), valueBits)
        res <- get(keyBits(0))
      } yield res

      whenReady(exec(prg))(_ shouldBe Some(valueBits))
    }

    "multiget" in {
      val prg = for {
        _ <- (1000 until 1010).toList.traverseU(x => put(keyBits(x), valueBits))
        res <- multiget((1000 until 1010).toList.map(keyBits))
      } yield res

      whenReady(exec(prg)) { res =>
        res should have size 10
      }
    }

    "pure" in {
      whenReady(exec(pure(1)))(_ shouldBe 1)
    }

    "fromTry - success" in {
      whenReady(exec(fromTry(Success(1))))(_ shouldBe 1)
    }
  }

  override protected def afterAll(): Unit = Files.walkFileTree(Paths.get(dir), new SimpleFileVisitor[Path] {
    override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
      Files.delete(file)
      FileVisitResult.CONTINUE
    }
  })
}
