package flumina.monix

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.scalatest.EitherMatchers
import com.typesafe.config.ConfigFactory
import flumina.akkaimpl.KafkaClient
import flumina.core.ir.{Compression, Record, TopicDescriptor}
import flumina.{KafkaDocker, KafkaDockerTest}
import monix.eval.Task
import monix.reactive.{Consumer, Observable, OverflowStrategy}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scodec.bits.ByteVector

import scala.concurrent.duration._

class MonixSpec extends Suite with WordSpecLike
    with KafkaDockerTest
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers
    with Inspectors
    with EitherMatchers
    with EitherValues
    with OptionValues {

  import monix.execution.Scheduler.Implicits.global

  val dir = "tests/target/test-offsets-data"

  "Monix" should {
    "produce and consume" in {
      val offsetStore = new RocksDbOffsetStore(dir)
      val parts = 10
      val nr = 100000l
      val topic = s"test${System.nanoTime()}"
      val producer = client.produce[Long, Long](topic, parts, OverflowStrategy.Unbounded, x => Record(ByteVector.empty, ByteVector(x.toByte)), identity, Compression.Snappy)

      val prg = for {
        _ <- Task.fromFuture(client.createTopics(Set(TopicDescriptor(topic, Some(parts), Some(1), Seq.empty, Map.empty))))
        _ <- Observable.range(0, nr).consumeWith(producer)
        _ <- client.consume(topic, ConsumptionStrategy.TerminateEndOfStream, offsetStore).consumeWith(Consumer.complete)
        offsets <- offsetStore.load(topic)
      } yield offsets

      whenReady(prg.runAsync)(_ should have size 10)
    }
  }

  final def kafkaVersion = "0.10.1.0"
  final def kafkaScaling = 3

  private def kafka1Port: Int = KafkaDocker.getPortFor("kafka", 1).getOrElse(sys.error("Unable to get port for kafka 1"))
  private def zookeeperPort: Int = 2181

  implicit lazy val system: ActorSystem = {
    val bootstrapBrokers = s"""{ host: "localhost", port: $kafka1Port }"""
    val bootstrapBrokersString = s"flumina.bootstrap-brokers = [$bootstrapBrokers]"
    val kafkaConfig = ConfigFactory.parseString(bootstrapBrokersString)

    ActorSystem("default", kafkaConfig.withFallback(ConfigFactory.load()))
  }

  private lazy val client = KafkaClient()(system, system.dispatcher)

  override implicit def patienceConfig = PatienceConfig(timeout = 60.seconds, interval = 10.milliseconds)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}