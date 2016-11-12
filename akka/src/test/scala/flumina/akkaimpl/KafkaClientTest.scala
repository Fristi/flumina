package flumina.akkaimpl

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.scalatest.{XorMatchers, XorValues}
import com.typesafe.config.ConfigFactory
import flumina.core.KafkaFailure
import flumina.core.ir._
import flumina.core.v090.Compression
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scodec.bits.ByteVector

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

abstract class KafkaClientTest extends Suite with WordSpecLike
    with KafkaDockerTest
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers
    with XorMatchers
    with XorValues
    with Inspectors
    with OptionValues {

  "KafkaClient" should {

    val testRecord = Record(ByteVector.empty, ByteVector("Hello world".getBytes()))

    "groupCoordinator" in new KafkaScope {
      whenReady(client.groupCoordinator("test")) { result =>
        result should be(right)
      }
    }

    "produceOne" in new KafkaScope {
      whenReady(client.produceOne(TopicPartitionValue(topicPartition, testRecord))) { result =>

        result.success should have size 1
        result.errors should have size 0

        result.success.head.topicPartition shouldBe topicPartition
        result.success.head.result shouldBe 0l
      }
    }

    "produceN & fetch" in new KafkaScope {

      val topicPartitionValue = TopicPartitionValue(topicPartition, testRecord)
      val prg = for {
        _ <- client.produceN(Compression.GZIP, List.tabulate(100)(_ => topicPartitionValue))
        fetchResult <- client.fetch(Set(TopicPartitionValue(topicPartition, 0l)))
      } yield fetchResult

      whenReady(prg) { result =>
        result.success should have size 1
        result.errors should have size 0

        result.success.head.result should have size 100
        result.success.head.result.head.record shouldBe testRecord

        forAll(result.success) { item =>
          respectOrder(item.result)(_.offset) shouldBe true
        }
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

      val topicPartitionValue = TopicPartitionValue(topicPartition, testRecord)

      val prg = for {
        _ <- fromAsync(client.produceN(Compression.Snappy, List(topicPartitionValue, topicPartitionValue)))
        gr <- fromAsyncXor(client.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Vector("test"), ByteVector.empty))))))
        _ <- fromAsyncXor(client.syncGroup(groupId, gr.generationId, gr.memberId, Seq(GroupAssignment(gr.memberId, memberAssignment))))
        fetchResult <- fromAsync(client.fetch(Set(TopicPartitionValue(topicPartition, 0l))))
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

    "listGroups" in new KafkaScope {
      whenReady(client.listGroups) { result =>
        result.value shouldNot have size 0
      }
    }
  }

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

  implicit lazy val system: ActorSystem = {
    def quote(str: String) = "\"" + str + "\""
    val bootstrapBrokers = List(s"localhost:$kafka1Port").map(quote).mkString(",")
    val bootstrapBrokersString = s"flumina.bootstrap-brokers = [$bootstrapBrokers]"
    val kafkaConfig = ConfigFactory.parseString(bootstrapBrokersString)

    ActorSystem("default", kafkaConfig.withFallback(ConfigFactory.load()))
  }

  private lazy val client = KafkaClient()

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

  override implicit def patienceConfig = PatienceConfig(timeout = 60.seconds, interval = 10.milliseconds)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
