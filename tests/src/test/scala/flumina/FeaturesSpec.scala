package flumina

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.Timeout
import cats.implicits._
import cats.scalatest.EitherMatchers
import com.typesafe.config.ConfigFactory
import flumina.client.KafkaClient
import flumina._
import flumina.monix.ConsumptionStrategy
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scodec.codecs._

import scala.concurrent.duration._
import flumina.monix._
import flumina.avro4s._
import _root_.monix.eval._
import _root_.monix.reactive._

class FeaturesSpec
    extends Suite
    with WordSpecLike
    with KafkaDockerTest
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers
    with Inspectors
    with EitherMatchers
    with EitherValues
    with OptionValues {

  import _root_.monix.execution.Scheduler.Implicits.global

  case class Person(name: String, age: Int)

  private val personTopic = s"person${System.nanoTime()}"

  private implicit val timeout: Timeout            = 3.seconds
  private implicit val longCodec: KafkaCodec[Long] = KafkaCodec.fromValueCodec(uint32)
  private implicit val personCodec: KafkaCodec[Person] =
    avroCodec[Person](personTopic, new MockSchemaRegistryClient())
  private implicit val personParitioner: KafkaPartitioner[Person] =
    KafkaPartitioner.stringPartitioner.contramap[Person](_.name)

  case class TestState(count: Int, order: Map[TopicPartition, List[Long]]) {
    def updateWith[A](topicPartitionValue: TopicPartitionValue[OffsetValue[A]]): TestState =
      copy(count = count + 1, order = order |+| Map(topicPartitionValue.topicPartition -> List(topicPartitionValue.result.offset)))
  }

  "Monix" should {
    "produce and consume" in {
      val parts    = 10
      val nr       = 100000l
      val topic    = s"test${System.nanoTime()}"
      val producer = client.produce[Long](topic = topic, nrPartitions = 10)

      val prg = for {
        _ <- Task.fromFuture(
          client.createTopics(topics = Set(TopicDescriptor(topic = topic, nrPartitions = Some(parts), replicationFactor = Some(1), replicaAssignment = Seq.empty, config = Map.empty))))
        _ <- Observable.range(0, nr).consumeWith(producer)
        testState <- client
          .messages[Long](topic, ConsumptionStrategy.TerminateEndOfStream)
          .consumeWith(Consumer.foldLeft(TestState(0, Map.empty))(_.updateWith(_)))
      } yield testState

      whenReady(prg.runAsync) {
        case (testState) =>
          testState.count shouldBe nr
          forAll(testState.order.values)(_ should TestUtils.respectOrder[Long](identity))
      }
    }
  }

  "Avro4s" should {
    "produce and consume" in {
      val parts = 1
      val nr    = 100000l

      val producer = client.produce[Person](topic = personTopic, nrPartitions = 1)

      val prg = for {
        _ <- Task.fromFuture(
          client.createTopics(topics = Set(TopicDescriptor(topic = personTopic, nrPartitions = Some(parts), replicationFactor = Some(1), replicaAssignment = Seq.empty, config = Map.empty))))
        _ <- Observable.range(0, nr).map(_ => Person("Mark", 30)).consumeWith(producer)
        testState <- client
          .messages[Person](personTopic, ConsumptionStrategy.TerminateEndOfStream)
          .consumeWith(Consumer.foldLeft(0l)((acc, _) => acc + 1l))
      } yield testState

      whenReady(prg.runAsync) { _ shouldBe nr }
    }
  }

  final def kafkaVersion = "0.10.1.0"
  final def kafkaScaling = 3

  private def kafka1Port: Int =
    KafkaDocker.getPortFor("kafka", 1).getOrElse(sys.error("Unable to get port for kafka 1"))

  implicit lazy val system: ActorSystem = {
    val bootstrapBrokers       = s"""{ host: "localhost", port: $kafka1Port }"""
    val bootstrapBrokersString = s"flumina.bootstrap-brokers = [$bootstrapBrokers]"
    val kafkaConfig            = ConfigFactory.parseString(bootstrapBrokersString)

    ActorSystem("default", kafkaConfig.withFallback(ConfigFactory.load()))
  }

  private lazy val client = KafkaClient()(system, system.dispatcher)

  override implicit def patienceConfig =
    PatienceConfig(timeout = 60.seconds, interval = 10.milliseconds)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
