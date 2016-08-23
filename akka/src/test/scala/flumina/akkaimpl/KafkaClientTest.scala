package flumina.akkaimpl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import cats.scalatest.{XorMatchers, XorValues}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import flumina.types.ir.{OffsetMetadata, Record, RecordEntry, TopicPartition}

import scala.concurrent.duration._

abstract class KafkaClientTest extends TestKit(ActorSystem())
    with WordSpecLike
    with BeforeAndAfter
    with BeforeAndAfterAll
    with ScalaFutures
    with KafkaDockerTest
    with Matchers
    with XorMatchers
    with XorValues
    with Inspectors
    with OptionValues {

  import system.dispatcher

  "KafkaClient" should {

    "produce and fetch (from/to) multiple partitions" in new KafkaScope {
      val nrPartitions = 10
      val name = randomTopic(partitions = nrPartitions, replicationFactor = 3)
      val size = 5000
      val produce = (1 to size)
        .map(x => TopicPartition(name, x % nrPartitions) -> Record.fromUtf8StringValue(s"Hello world $x"))
        .toMultimap

      val prg = for {
        produceResult <- client.produce(produce)
        _ <- FutureUtils.delay(1.second)
        fetchResult <- client.singleFetch(TopicPartition.enumerate(name, nrPartitions).map(_ -> 0l).toMap)
      } yield produceResult -> fetchResult

      whenReady(prg) {
        case (produceResult, fetchResult) =>
          //check if the produceResult has error
          produceResult.errors should have size 0
          produceResult.success should have size nrPartitions.toLong

          //check if the fetchResult has error
          fetchResult.errors should have size 0
          fetchResult.success should have size nrPartitions.toLong
          //it should be evenly divided
          forAll(fetchResult.success) { y => y.value.size shouldBe (size / nrPartitions).toLong }
      }
    }

    "fetch all messages" in new KafkaScope {
      val nrPartitions = 10
      val name = randomTopic(partitions = nrPartitions, replicationFactor = 3)
      val nrToProduce = 100000
      val produce = (1 to nrToProduce)
        .map(x => TopicPartition(name, x % nrPartitions) -> Record.fromUtf8StringValue(s"Hello world $x"))
        .toMultimap

      whenReady(client.produce(produce).flatMap(x => FutureUtils.delay(1.seconds).map(_ => x))) { produceResult =>
        //check if the produceResult has error
        produceResult.errors should have size 0
        produceResult.success should have size nrPartitions.toLong

        client.fetchFromBeginning(TopicPartition.enumerate(name, nrPartitions))
          .runWith(TestSink.probe[RecordEntry])
          .ensureSubscription()
          .request(nrToProduce.toLong)
          .expectNextN(nrToProduce.toLong) should have size nrToProduce.toLong
      }
    }

    "commit and fetch offsets" in new KafkaScope {
      val nrPartitions = 10
      val name = randomTopic(partitions = nrPartitions, replicationFactor = 3)
      val size = 1000
      val produce = (1 to size)
        .map(x => TopicPartition(name, x % nrPartitions) -> Record.fromUtf8StringValue(s"Hello world $x"))
        .toMultimap

      val prg = for {
        produceResult <- client.produce(produce)
        _ <- FutureUtils.delay(1.second)
        fetchResult <- client.singleFetch(TopicPartition.enumerate(name, nrPartitions).map(_ -> 0l).toMap)
        commitRequest = fetchResult.success.map(x => x.topicPartition -> OffsetMetadata(x.value.maxBy(y => y.offset).offset, None)).toMap
        offsetCommitResult <- client.offsetsCommit("test", commitRequest)
        offsetFetchResult <- client.offsetsFetch("test", TopicPartition.enumerate(name, nrPartitions))
      } yield offsetCommitResult -> offsetFetchResult

      whenReady(prg) {
        case (offsetCommitResult, offsetFetchResult) =>
          offsetCommitResult.errors should have size 0
          offsetCommitResult.success should have size nrPartitions.toLong

          offsetFetchResult.errors should have size 0
          offsetFetchResult.success should have size nrPartitions.toLong

          forAll(offsetFetchResult.success) { x => x.value.offset shouldBe 99l }
      }
    }
  }

  final def kafkaScaling = 3

  private def kafka1Port: Int = KafkaDocker.getPortFor("kafka", 1).getOrElse(sys.error("Unable to get port for kafka 1"))
  private def zookeeperPort: Int = 2181

  private def deadServer(nr: Int) = KafkaBroker.Node(s"localhost", 12300 + nr)

  private lazy val settings = KafkaSettings(
    bootstrapBrokers = Seq(KafkaBroker.Node("localhost", kafka1Port)),
    //    bootstrapBrokers = Seq(deadServer(1), deadServer(2), KafkaBroker.Node("localhost", kafka1Port)),
    connectionsPerBroker = 1,
    //    connectionsPerBroker = 3,
    operationalSettings = KafkaOperationalSettings(
      retryBackoff = 500.milliseconds,
      retryMaxCount = 5,
      fetchMaxBytes = 32 * 1024,
      fetchMaxWaitTime = 20.seconds,
      produceTimeout = 20.seconds,
      groupSessionTimeout = 30.seconds
    ),
    requestTimeout = 30.seconds
  )

  private lazy val client = KafkaClient(settings)

  trait KafkaScope {
    def randomTopic(partitions: Int, replicationFactor: Int) = {
      val name = s"test${System.nanoTime().toString}"
      Utils.createTopic(name, partitions, replicationFactor, zookeeperPort)
      Thread.sleep(1000)
      name
    }
  }

  private implicit val actorMaterializer = ActorMaterializer()(system)

  override implicit def patienceConfig = PatienceConfig(timeout = 30.seconds, interval = 10.milliseconds)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
