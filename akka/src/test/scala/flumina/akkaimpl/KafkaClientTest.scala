package flumina.akkaimpl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import cats.scalatest.{XorMatchers, XorValues}
import flumina.core.KafkaFailure
import flumina.core.ir._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scodec.bits.ByteVector

import scala.annotation.tailrec
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

    val testRecord = Record(ByteVector.empty, ByteVector("Hello world".getBytes()))

    "groupCoordinator" in new KafkaScope {
      whenReady(client.groupCoordinator("test")) { result =>
        result should be(right)
      }
    }

    "produce" in new KafkaScope {
      whenReady(client.produce(List(topicPartition -> testRecord))) { result =>
        result.success should have size 1
        result.errors should have size 0

        result.success.head.topicPartition shouldBe topicPartition
        result.success.head.result shouldBe 0l
      }
    }

    "fetch" in new KafkaScope {
      val prg = for {
        _ <- client.produce(List(topicPartition -> testRecord))
        fetchResult <- client.fetch(Map(topicPartition -> 0l))
      } yield fetchResult

      whenReady(prg) { result =>
        result.success should have size 1
        result.errors should have size 0

        result.success.head.result should have size 1
        result.success.head.result.head.record shouldBe testRecord
      }
    }

    "metadata" in new KafkaScope {
      whenReady(client.metadata(Set.empty)) { result =>
        result.brokers shouldNot have size 0
        result.topics.value.isRight shouldBe true
      }
    }

    "joinGroup" in new KafkaScope {
      whenReady(client.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Vector("test"), ByteVector.empty)))))) { result =>
        val joinGroupResult = result.value

        joinGroupResult.memberId should not be empty
        joinGroupResult.groupProtocol should not be empty
        joinGroupResult.leaderId should not be empty
        joinGroupResult.members should have size 1
        joinGroupResult.members.head.memberId shouldBe joinGroupResult.memberId
      }
    }

    "offsetCommit and offsetFetch" in new KafkaScope {
      import KafkaFailure._

      val memberAssignment = MemberAssignment(
        version = 0,
        topicPartitions = Seq(TopicPartition("test", 0)),
        userData = ByteVector("more-test".getBytes)
      )

      val prg = for {
        produceResult <- fromAsync(client.produce(List(topicPartition -> testRecord, topicPartition -> testRecord)))
        gr <- fromAsyncXor(client.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Vector("test"), ByteVector.empty))))))
        _ <- fromAsyncXor(client.syncGroup(groupId, gr.generationId, gr.memberId, Seq(GroupAssignment(gr.memberId, memberAssignment))))
        fetchResult <- fromAsync(client.fetch(Map(topicPartition -> 0l)))
        offsetCommitResult <- fromAsync(client.offsetCommit(groupId, gr.generationId, gr.memberId, Map(topicPartition -> OffsetMetadata(fetchResult.success.head.result.maxBy(_.offset).offset, Some("metadata")))))
        offsetFetchResult <- fromAsync(client.offsetFetch(groupId, Set(topicPartition)))
      } yield offsetCommitResult -> offsetFetchResult

      whenReady(prg.value) { result =>
        val (offsetCommitResult, offsetFetchResult) = result.value

        offsetCommitResult.errors should have size 0
        offsetCommitResult.success should have size 1
        offsetCommitResult.success.head.topicPartition shouldBe topicPartition

        offsetFetchResult.errors should have size 0
        offsetFetchResult.success should have size 1
        offsetFetchResult.success.head.topicPartition shouldBe topicPartition
        offsetFetchResult.success.head.result.offset shouldBe 1
      }
    }

    "leaveGroup" in new KafkaScope {
      import KafkaFailure._

      val prg = for {
        joinGroupResult <- fromAsyncXor(client.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Seq("test"), ByteVector.empty))))))
        leaveGroupResult <- fromAsyncXor(client.leaveGroup(groupId, joinGroupResult.memberId))
      } yield leaveGroupResult

      whenReady(prg.value) { result =>
        result should be(right)
      }
    }

    "heartbeat" in new KafkaScope {
      import KafkaFailure._

      val prg = for {
        joinGroupResult <- fromAsyncXor(client.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Seq("test"), ByteVector.empty))))))
        syncGroupResult <- fromAsyncXor(
          client.syncGroup(
            groupId = groupId,
            generationId = joinGroupResult.generationId,
            memberId = joinGroupResult.memberId,
            assignments = Seq(
              GroupAssignment(
                memberId = joinGroupResult.memberId,
                memberAssignment = MemberAssignment(
                  version = 0,
                  topicPartitions = Seq(TopicPartition(topic = "test", partition = 0)),
                  userData = ByteVector("more-test".getBytes)
                )
              )
            )
          )
        )
        heartbeatResult <- fromAsyncXor(client.heartbeat(groupId, joinGroupResult.generationId, joinGroupResult.memberId))
      } yield heartbeatResult

      whenReady(prg.value) { result =>
        result should be(right)
      }
    }

    "staged produce and consume" in new KafkaScope {
      val group = s"test${System.currentTimeMillis()}"
      val (topic1name, topic1parts) = randomTopic(partitions = 10, replicationFactor = 1)
      val (topic2name, topic2parts) = randomTopic(partitions = 4, replicationFactor = 1)
      val size = 100000
      val producer = client.producer(grouped = 500, parallelism = 5)

      Source(1 to size)
        .map(x => TopicPartition(topic1name, x % 10) -> testRecord)
        .runWith(producer)

      client.consume(groupId = s"${group}_a", topic1parts)
        .filter(x => x.recordEntry.offset % 16 < 4)
        .map(_.recordEntry.offset)
        .map(x => TopicPartition(topic2name, x.toInt % 4) -> testRecord)
        .runWith(producer)

      val results = client.consume(groupId = s"${group}_b", topic2parts)
        .runWith(TestSink.probe[TopicPartitionRecordEntry])
        .ensureSubscription()
        .request(25000)
        .expectNextN(25000)

      def prg = for {
        offsetGroupA <- client.offsetFetch(s"${group}_a", topic1parts)
        offsetGroupB <- client.offsetFetch(s"${group}_b", topic2parts)
      } yield offsetGroupA -> offsetGroupB

      results.distinct should have size 25000

      forAll(results.groupBy(_.topicPartition).values) { items =>
        items should have size 6250
        respectOrder(items)(x => x.recordEntry.offset) shouldBe true
      }

      Thread.sleep(500)

      whenReady(prg) {
        case (offsetGroupA, offsetGroupB) =>
          offsetGroupA.errors should have size 0
          offsetGroupA.success should have size 10

          forAll(offsetGroupA.success)(_.result.offset shouldBe 9999l)

          offsetGroupB.errors should have size 0
          offsetGroupB.success should have size 4

          forAll(offsetGroupB.success)(_.result.offset shouldBe 6249l)
      }
    }

    "listGroups" in new KafkaScope {
      import KafkaFailure._

      val prg = for {
        _ <- fromAsyncXor(client.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Seq("test"), ByteVector.empty))))))
        groups <- fromAsyncXor(client.listGroups)
      } yield groups

      whenReady(prg.value) { result =>
        result.value shouldNot have size 0
      }
    }

    "multiple joining consumers should rebalance" in new KafkaScope {

      import KafkaFailure._

      val (topicName, _) = randomTopic(1, 1)
      val groupProtocols1 = Seq(GroupProtocol("range", Seq(ConsumerProtocol(0, Seq(topicName), ByteVector("Test".getBytes())))))
      val groupProtocols2 = Seq(GroupProtocol("range", Seq(ConsumerProtocol(0, Seq(topicName), ByteVector("Test".getBytes())))))
      val group = s"test${System.currentTimeMillis()}"
      val memberAssignment = MemberAssignment(
        version = 0,
        topicPartitions = Seq(TopicPartition(topicName, 0)),
        userData = ByteVector("more-test".getBytes)
      )

      val prg = for {
        joinGroupResult1 <- fromAsyncXor(client.joinGroup(groupId = group, memberId = None, protocol = "consumer", protocols = groupProtocols1))
        syncGroupResult1 <- fromAsyncXor(client.syncGroup(groupId = group, generationId = joinGroupResult1.generationId, memberId = joinGroupResult1.memberId, assignments = Seq(GroupAssignment(joinGroupResult1.memberId, memberAssignment))))
        joinGroupResult2 <- fromAsyncXor(client.joinGroup(groupId = group, memberId = None, protocol = "consumer", protocols = groupProtocols2))
        syncGroupResult2 <- fromAsyncXor(client.syncGroup(groupId = group, generationId = joinGroupResult2.generationId, memberId = joinGroupResult2.memberId, assignments = Seq(GroupAssignment(joinGroupResult2.memberId, memberAssignment))))
      } yield joinGroupResult2 -> syncGroupResult2

      whenReady(prg.value) { result =>
        result should be(right)
      }
    }
  }

  def counter(name: String) = Flow[TopicPartitionRecordEntry]
    .grouped(500)
    .scan(0)((acc, items) => acc + items.size)
    .to(Sink.foreach(c => println(s"$name count: $c")))
    .async

  def respectOrder[A](items: Seq[A])(f: (A => Long)) = {
    @tailrec
    def loop(list: List[A]): Boolean = list match {
      case first :: second :: tail => if (f(first) < f(second)) loop(second :: tail) else false
      case _                       => true
    }

    loop(items.toList)
  }

  final def kafkaScaling = 3

  private def kafka1Port: Int = KafkaDocker.getPortFor("kafka", 1).getOrElse(sys.error("Unable to get port for kafka 1"))
  private def zookeeperPort: Int = 2181

  private def deadServer(nr: Int) = KafkaBroker.Node(s"localhost", 12300 + nr)

  private lazy val settings = KafkaSettings(
    bootstrapBrokers = Seq(deadServer(1), deadServer(2), KafkaBroker.Node("localhost", kafka1Port)),
    connectionsPerBroker = 1,
    operationalSettings = KafkaOperationalSettings(
      retryBackoff = 500.milliseconds,
      retryMaxCount = 10,
      fetchMaxBytes = 32 * 1024,
      fetchMaxWaitTime = 100.milliseconds,
      produceTimeout = 1.seconds,
      groupSessionTimeout = 10.seconds,
      heartbeatFrequency = 4,
      consumeAssignmentStrategy = ConsumeAssignmentStrategy.allToLeader
    ),
    requestTimeout = 30.seconds
  )

  private lazy val client = KafkaClient(settings)

  trait KafkaScope {
    def randomTopic(partitions: Int, replicationFactor: Int) = {
      val name = s"test${System.nanoTime().toString}"
      Utils.createTopic(name, partitions, replicationFactor, zookeeperPort)
      Thread.sleep(500)
      name -> TopicPartition.enumerate(name, partitions)
    }

    private def randomGroup = s"group${System.nanoTime().toString}"

    lazy val groupId = randomGroup
    lazy val topicPartition = TopicPartition(randomTopic(1, 1)._1, 0)
  }

  private implicit val actorMaterializer = ActorMaterializer()(system)

  override implicit def patienceConfig = PatienceConfig(timeout = 60.seconds, interval = 10.milliseconds)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
