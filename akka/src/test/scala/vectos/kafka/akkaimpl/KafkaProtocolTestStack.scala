package vectos.kafka.akkaimpl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, WordSpecLike}
import vectos.kafka.akkaimpl.Kafka.Context
import vectos.kafka.akkaimpl.versions.KafkaMonad
import vectos.kafka.types.ir.TopicPartition
import vectos.kafka.types.{KafkaAlg, kafka}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

abstract class KafkaProtocolTestStack extends TestKit(ActorSystem()) with WordSpecLike with BeforeAndAfter with BeforeAndAfterAll with ScalaFutures {

  val protocolVersion: Int
  val protocol: KafkaAlg[KafkaMonad]

  trait KafkaScope {
    private def randomTopic = {
      val name = s"test${System.nanoTime().toString}"
      EmbeddedKafka.createCustomTopic(name)
      Thread.sleep(500)
      name
    }

    private def randomGroup = s"group${System.nanoTime().toString}"

    lazy val groupId = randomGroup
    lazy val topicPartition = TopicPartition(randomTopic, 0)
    lazy val context: Context = Context(
      connection = system.actorOf(KafkaConnection.props(KafkaConnection.Settings("localhost", 9999, 1000))),
      requestTimeout = Timeout(10.seconds),
      settings = Kafka.Settings(
        retryBackoffMs = 300,
        retryMaxCount = 10,
        fetchMaxBytes = 8 * 1024,
        fetchMaxWaitTime = 20000,
        produceTimeout = 20000,
        groupSessionTimeout = 30000
      )
    )

    def run[T](kafkaDsl: kafka.Dsl[T]) = kafkaDsl(protocol).value.run(context)
  }

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 9999, customBrokerProperties = Map("auto.create.topics.enable" -> "true"))
  implicit val materializer: ActorMaterializer = ActorMaterializer()(system)
  implicit val executionContext: ExecutionContext = system.dispatcher

  override implicit def patienceConfig = PatienceConfig(timeout = 10.seconds, interval = 10.milliseconds)

  override protected def beforeAll(): Unit = {
    EmbeddedKafka.start()

  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    EmbeddedKafka.stop()
  }
}
