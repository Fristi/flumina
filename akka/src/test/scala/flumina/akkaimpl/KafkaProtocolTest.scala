package flumina.akkaimpl

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.Timeout
import cats.scalatest._
import cats.syntax.all._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, OptionValues, WordSpecLike}
import flumina.akkaimpl.versions.KafkaMonad
import flumina.types.ir._
import flumina.types.{KafkaAlg, KafkaResult, kafka}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

abstract class KafkaProtocolTest extends TestKit(ActorSystem()) with WordSpecLike with ScalaFutures with KafkaDockerTest with Matchers with XorMatchers with XorValues with OptionValues {
  s"Kafka" should {

    "groupCoordinator" in new KafkaScope {
      whenReady(run(kafka.groupCoordinator("test"))) { result =>
        val groupCoordinator = result.value

        groupCoordinator.nodeId shouldBe 1001
        groupCoordinator.port shouldBe kafkaPort
      }
    }
    "produce" in new KafkaScope {
      whenReady(run(kafka.produce(Map(topicPartition -> List(Record.fromUtf8StringKeyValue("key", "Hello world")))))) { result =>
        val produceResult = result.value

        produceResult should have size 1
        produceResult.containsError shouldBe false
        produceResult.head.topicPartition shouldBe topicPartition
        produceResult.head.value shouldBe 0
      }
    }

    "fetch" in new KafkaScope {
      val prg = for {
        _ <- kafka.produce(Map(topicPartition -> List(Record.fromUtf8StringKeyValue("key", "Hello world"))))
        fetchResult <- kafka.fetch(Map(topicPartition -> 0l))
      } yield fetchResult

      whenReady(run(prg)) { result =>
        val fetchResult = result.value

        fetchResult should have size 1
        fetchResult.containsError shouldBe false
        fetchResult.head.topicPartition shouldBe topicPartition
        fetchResult.head.value should have size 1
        fetchResult.head.value.head shouldBe RecordEntry(0, Record.fromUtf8StringKeyValue("key", "Hello world"))
      }
    }

    "joinGroup" in new KafkaScope {
      whenReady(run(kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Vector("test"), Seq.empty))))))) { result =>
        val joinGroupResult = result.value

        joinGroupResult.memberId should not be empty
        joinGroupResult.groupProtocol should not be empty
        joinGroupResult.leaderId should not be empty
        joinGroupResult.members should have size 1
        joinGroupResult.members.head.memberId shouldBe joinGroupResult.memberId

      }
    }

    "offsetCommit and offsetFetch" in new KafkaScope {
      val prg = for {
        produceResult <- kafka.produce(Map(topicPartition -> List(Record.fromUtf8StringValue("Hello world"), Record.fromUtf8StringValue("Hello world"))))
        joinGroupResult <- kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Vector("test"), Seq.empty)))))
        fetchResult <- kafka.fetch(Map(topicPartition -> 0l))
        offsetCommitResult <- kafka.offsetCommit(groupId, Map(topicPartition -> OffsetMetadata(fetchResult.head.value.maxBy(_.offset).offset, Some("metadata"))))
        offsetFetchResult <- kafka.offsetFetch(groupId, Set(topicPartition))
      } yield offsetCommitResult -> offsetFetchResult

      whenReady(run(prg)) { result =>
        val (offsetCommitResult, offsetFetchResult) = result.value

        offsetCommitResult should have size 1
        offsetFetchResult should have size 1
        offsetFetchResult.head.kafkaResult shouldBe KafkaResult.NoError
        offsetFetchResult.head.topicPartition shouldBe topicPartition
        offsetFetchResult.head.value.offset shouldBe 1
      }
    }

    "describeGroups with syncGroup call" in new KafkaScope {
      val memberAssignment = MemberAssignment(
        version = 0,
        topicPartitions = Seq(TopicPartition("test", 0)),
        userData = "more-test".getBytes
      )

      val consumerProtocolMetadata = ConsumerProtocol(0, Seq("test"), "test".getBytes)

      val prg = for {
        joinGroupResult <- kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(consumerProtocolMetadata))))
        _ <- kafka.syncGroup(
          groupId = groupId,
          generationId = joinGroupResult.generationId,
          memberId = joinGroupResult.memberId,
          assignments = Seq(
            GroupAssignment(
              memberId = joinGroupResult.memberId,
              memberAssignment = memberAssignment
            )
          )
        )
        describeGroupResult <- kafka.describeGroups(Set(groupId))
      } yield joinGroupResult -> describeGroupResult

      whenReady(run(prg)) { result =>
        val (joinGroupResult, describeGroupResult) = result.value

        describeGroupResult should have size 1
        describeGroupResult.head.kafkaResult shouldBe KafkaResult.NoError
        describeGroupResult.head.groupId shouldBe groupId
        describeGroupResult.head.state shouldBe "Stable"
        describeGroupResult.head.protocolType should not be empty
        describeGroupResult.head.members should have size 1
        //        describeGroupResult.head.members.head.clientHost shouldBe Some("/127.0.0.1")
        describeGroupResult.head.members.head.memberId shouldBe joinGroupResult.memberId
        describeGroupResult.head.members.head.consumerProtocol shouldBe Some(consumerProtocolMetadata)
        describeGroupResult.head.members.head.assignment shouldBe Some(memberAssignment)
      }
    }

    "describeGroups without syncGroup call" in new KafkaScope {
      val memberAssignment = MemberAssignment(
        version = 0,
        topicPartitions = Seq(TopicPartition("test", 0)),
        userData = "more-test".getBytes
      )

      val consumerProtocolMetadata = ConsumerProtocol(0, Seq("test"), "test".getBytes)

      val prg = for {
        joinGroupResult <- kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(consumerProtocolMetadata))))
        describeGroupResult <- kafka.describeGroups(Set(groupId))
      } yield joinGroupResult -> describeGroupResult

      whenReady(run(prg)) { result =>
        val (joinGroupResult, describeGroupResult) = result.value

        describeGroupResult should have size 1
        describeGroupResult.head.kafkaResult shouldBe KafkaResult.NoError
        describeGroupResult.head.groupId shouldBe groupId
        describeGroupResult.head.state shouldBe "AwaitingSync"
        describeGroupResult.head.protocolType should not be empty
        describeGroupResult.head.members should have size 1
        describeGroupResult.head.members.head.memberId shouldBe joinGroupResult.memberId
        describeGroupResult.head.members.head.consumerProtocol shouldBe None
        describeGroupResult.head.members.head.assignment shouldBe None
      }
    }

    "leaveGroup" in new KafkaScope {
      val prg = for {
        joinGroupResult <- kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Seq("test"), Seq.empty)))))
        leaveGroupResult <- kafka.leaveGroup(groupId, joinGroupResult.memberId)
      } yield leaveGroupResult

      whenReady(run(prg)) { result =>
        result should be(right)
      }
    }

    "heartbeat" in new KafkaScope {
      val prg = for {
        joinGroupResult <- kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Seq("test"), Seq.empty)))))
        heartbeatResult <- kafka.heartbeat(groupId, joinGroupResult.generationId, joinGroupResult.memberId)
      } yield heartbeatResult

      whenReady(run(prg)) { result =>
        result should be(left) //TODO: rebalance in progress??,, should it delay??
      }
    }

    "syncGroup" in new KafkaScope {

      val prg = for {
        joinGroupResult <- kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Seq("test"), "test".getBytes)))))
        syncGroupResult <- kafka.syncGroup(
          groupId = groupId,
          generationId = joinGroupResult.generationId,
          memberId = joinGroupResult.memberId,
          assignments = Seq(
            GroupAssignment(
              memberId = joinGroupResult.memberId,
              memberAssignment = MemberAssignment(
                version = 0,
                topicPartitions = Seq(TopicPartition(topic = "test", partition = 0)),
                userData = "more-test".getBytes
              )
            )
          )
        )
      } yield syncGroupResult

      whenReady(run(prg)) { result =>
        val syncGroupResult = result.value
        syncGroupResult.version shouldBe 0
        syncGroupResult.topicPartitions should have size 1
        syncGroupResult.topicPartitions.head shouldBe TopicPartition("test", 0)
        syncGroupResult.userData shouldBe "more-test".getBytes
      }
    }

    "metadata" in new KafkaScope {
      whenReady(run(kafka.metadata(Set.empty))) { result =>
        val metadata = result.value

        metadata.brokers should have size 1
        metadata.brokers.head.nodeId shouldBe 1001
        metadata.brokers.head.port shouldBe kafkaPort

        metadata.metadata shouldNot have size 0
      }
    }

    "listOffsets" in new KafkaScope {
      val prg = for {
        produceResult <- kafka.produce(Map(topicPartition -> List(Record.fromUtf8StringKeyValue("key", "Hello world"), Record.fromUtf8StringKeyValue("key", "Hello world"))))
        fetchResult <- kafka.fetch(Map(topicPartition -> 0l))
        offsetCommitResult <- kafka.offsetCommit(groupId, Map(topicPartition -> OffsetMetadata(fetchResult.head.value.maxBy(_.offset).offset, Some("metadata"))))
        listOffsetResult <- kafka.listOffsets(Set(topicPartition))
      } yield listOffsetResult

      whenReady(run(prg)) { result =>
        val listOffsetResult = result.value
        listOffsetResult should have size 1
        listOffsetResult.head.value shouldBe Vector(2)
        listOffsetResult.head.topicPartition shouldBe topicPartition
        listOffsetResult.head.kafkaResult shouldBe KafkaResult.NoError
      }
    }

    "listGroups" in new KafkaScope {
      whenReady(run(kafka.listGroups)) { result =>
        result.value shouldNot have size 0
      }
    }
  }

  final def kafkaScaling = 1

  val protocol: KafkaAlg[KafkaMonad]

  private def kafkaPort: Int = KafkaDocker.getPortFor("kafka", 1).getOrElse(sys.error("Could not get port for kafka 1"))
  private def zookeeperPort: Int = 2181

  private lazy val brokerRouter = system.actorOf(
    KafkaConnectionPool.props(
      bootstrapBrokers = Seq(deadServer(1), deadServer(2), KafkaBroker.Node("localhost", kafkaPort)),
      connectionsPerBroker = 1
    )
  )

  private def deadServer(nr: Int) = KafkaBroker.Node(s"localhost", 12300 + nr)

  trait KafkaScope {
    private def randomTopic = {
      val name = s"test${System.nanoTime().toString}"
      Utils.createTopic(name, 1, 1, zookeeperPort)
      Thread.sleep(500)
      name
    }

    private def randomGroup = s"group${System.nanoTime().toString}"

    lazy val groupId = randomGroup
    lazy val topicPartition = TopicPartition(randomTopic, 0)
    lazy val context: KafkaContext = KafkaContext(
      connectionPool = brokerRouter,
      broker = KafkaBroker.AnyNode,
      requestTimeout = Timeout(30.seconds),
      settings = KafkaOperationalSettings(
        retryBackoff = 500.milliseconds,
        retryMaxCount = 5,
        fetchMaxBytes = 8 * 1024,
        fetchMaxWaitTime = 20.seconds,
        produceTimeout = 20.seconds,
        groupSessionTimeout = 30.seconds
      )
    )

    def run[T](kafkaDsl: kafka.Dsl[T]) = kafkaDsl(protocol).value.run(context)
  }

  implicit val executionContext: ExecutionContext = system.dispatcher

  override implicit def patienceConfig = PatienceConfig(timeout = 30.seconds, interval = 10.milliseconds)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
