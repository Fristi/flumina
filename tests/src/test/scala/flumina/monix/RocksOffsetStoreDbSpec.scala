package flumina.monix

import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import cats.scalatest.EitherMatchers
import flumina.core.ir.{TopicPartition, TopicPartitionValue}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

class RocksOffsetStoreDbSpec extends Suite with WordSpecLike with ScalaFutures
    with Matchers
    with Inspectors
    with EitherMatchers
    with EitherValues
    with OptionValues
    with BeforeAndAfterAll {

  import monix.execution.Scheduler.Implicits.global

  val dir = "tests/target/offsets-data"
  val rocksDb = new RocksDbOffsetStore(dir)
  def topicPartition(part: Int) = TopicPartitionValue(TopicPartition("test", part), 0l)

  "RocksDbOffsetStore" should {
    "save and load" in {
      val prg = for {
        _ <- rocksDb.save(topicPartition(0))
        _ <- rocksDb.save(topicPartition(1))
        _ <- rocksDb.save(topicPartition(2))
        res <- rocksDb.load("test")
      } yield res

      whenReady(prg.runAsync)(_ shouldBe Vector(topicPartition(0), topicPartition(1), topicPartition(2)))
    }
  }

  override protected def afterAll(): Unit = Files.walkFileTree(Paths.get(dir), new SimpleFileVisitor[Path] {
    override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
      Files.delete(file)
      FileVisitResult.CONTINUE
    }
  })
}
