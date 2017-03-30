package flumina.akkaimpl

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.data.EitherT
import cats.free.Free
import cats.implicits._
import cats.scalatest.EitherMatchers
import com.typesafe.config.ConfigFactory
import flumina.{KafkaDocker, KafkaDockerTest}
import flumina.core._
import flumina.core.ir._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import scodec.bits.ByteVector

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

abstract class KafkaClientTest extends Suite with WordSpecLike
    with KafkaDockerTest
    with BeforeAndAfterAll
    with ScalaFutures
    with Matchers
    with Inspectors
    with EitherMatchers
    with EitherValues
    with OptionValues {

  "KafkaClient" should {

    val testRecord = Record(key = ByteVector.empty, value = ByteVector("Hello world".getBytes()))

    "groupCoordinator" in new KafkaScope {
      whenReady(run(kafka.groupCoordinator("test"))) { result =>
        result should be(right)
      }
    }

    "produceOne" in new KafkaScope {
      val topicPartition = TopicPartition(topic = topic, partition = 0)
      val prg = for {
        res <- kafka.createTopics(Set(TopicDescriptor(topic = topic, nrPartitions = Some(3), replicationFactor = Some(1), replicaAssignment = Seq.empty, config = Map())))
        result <- kafka.produceOne(TopicPartitionValue(topicPartition, testRecord))
      } yield result

      whenReady(run(prg)) { result =>

        result.success should have size 1
        result.errors should have size 0

        result.success.head.topicPartition shouldBe topicPartition
        result.success.head.result shouldBe 0l
      }
    }

    "produceN & fetch" in new KafkaScope {
      val prg = for {
        _ <- kafka.createTopics(Set(TopicDescriptor(topic = topic, nrPartitions = Some(3), replicationFactor = Some(1), replicaAssignment = Seq.empty, config = Map())))
        _ <- kafka.produceN(Compression.GZIP, List.tabulate(60)(i => TopicPartitionValue(TopicPartition(topic, i % 3), testRecord)))
        fetchResult <- kafka.fetch(Set(TopicPartitionValue(TopicPartition(topic, 0), 0l)))
      } yield fetchResult

      whenReady(run(prg)) { result =>
        result.success should have size 1
        result.errors should have size 0

        result.success.head.result should have size 20
        result.success.head.result.head.record shouldBe testRecord

        forAll(result.success) { item =>
          respectOrder(item.result)(_.offset) shouldBe true
        }
      }
    }

    "joinGroup" in new KafkaScope {
      whenReady(run(kafka.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Vector("test"), ByteVector.empty))))))) { result =>
        val joinGroupResult = result.right.value

        joinGroupResult.memberId should not be empty
        joinGroupResult.groupProtocol should not be empty
        joinGroupResult.leaderId should not be empty
        joinGroupResult.members should have size 1
        joinGroupResult.members.head.memberId shouldBe joinGroupResult.memberId
      }
    }

    "offsetCommit and offsetFetch" in new KafkaScope {

      val memberAssignment = MemberAssignment(
        version = 0,
        topicPartitions = Seq(TopicPartition("test", 0)),
        userData = ByteVector("more-test".getBytes)
      )

      val topicPartition = TopicPartition(topic, 0)
      val topicPartitionValue = TopicPartitionValue(topicPartition, testRecord)

      val prg = for {
        _ <- pure(client.createTopics(Set(TopicDescriptor(topic = topic, nrPartitions = Some(3), replicationFactor = Some(1), replicaAssignment = Seq.empty, config = Map()))))
        _ <- pure(client.produceN(Compression.Snappy, List(topicPartitionValue, topicPartitionValue)))
        gr <- fromEither(client.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Vector("test"), ByteVector.empty))))))
        _ <- fromEither(client.syncGroup(groupId, gr.generationId, gr.memberId, Seq(GroupAssignment(gr.memberId, memberAssignment))))
        fetchResult <- pure(client.fetch(Set(TopicPartitionValue(topicPartition, 0l))))
        offsetCommitResult <- pure(client.offsetCommit(groupId, gr.generationId, gr.memberId, Map(topicPartition -> OffsetMetadata(fetchResult.success.head.result.maxBy(_.offset).offset, Some("metadata")))))
        offsetFetchResult <- pure(client.offsetFetch(groupId, Set(topicPartition)))
      } yield offsetCommitResult -> offsetFetchResult

      whenReady(prg.value) { result =>
        val (offsetCommitResult, offsetFetchResult) = result.right.value

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
      val prg = for {
        joinGroupResult <- fromEither(client.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Seq("test"), ByteVector.empty))))))
        leaveGroupResult <- fromEither(client.leaveGroup(groupId, joinGroupResult.memberId))
      } yield leaveGroupResult

      whenReady(prg.value) { result =>
        result should be(right)
      }
    }

    "heartbeat" in new KafkaScope {

      val prg = for {
        joinGroupResult <- fromEither(client.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocol(0, Seq("test"), ByteVector.empty))))))
        syncGroupResult <- fromEither(
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
        heartbeatResult <- fromEither(client.heartbeat(groupId, joinGroupResult.generationId, joinGroupResult.memberId))
      } yield heartbeatResult

      whenReady(prg.value) { result =>
        result should be(right)
      }
    }

    "listGroups" in new KafkaScope {
      whenReady(run(kafka.listGroups)) { result =>
        result.right.value shouldNot have size 0
      }
    }

    "metadata" in new KafkaScope {
      whenReady(run(kafka.metadata(Set.empty))) { result =>
        result.brokers shouldNot have size 0
        result.topics shouldNot have size 0
        result.topicsInError should have size 0
      }
    }

    "create and deleteTopic" in new KafkaScope {
      val prg = for {
        _ <- kafka.createTopics(Set(TopicDescriptor(topic, Some(3), Some(1), Seq.empty, Map())))
        topicMetadata <- kafka.metadata(Set(topic))
        _ <- kafka.deleteTopics(Set(topic))
      } yield topicMetadata

      whenReady(run(prg)) {
        case (topicMetadata) =>
          topicMetadata.topics.map(_.topicPartition.topic) should contain(topic)
      }
    }

    "describeGroups with syncGroup call" in new KafkaScope {
      val memberAssignment = MemberAssignment(
        version = 0,
        topicPartitions = Seq(TopicPartition("test", 0)),
        userData = ByteVector("more-test".getBytes)
      )

      val consumerProtocolMetadata = ConsumerProtocol(0, Seq("test"), ByteVector("test".getBytes))

      val prg = for {
        joinGroupResult <- fromEither(client.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(consumerProtocolMetadata)))))
        _ <- pure(
          client.syncGroup(
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
        )
        describeGroupResult <- pure(client.describeGroups(Set(groupId)))
      } yield joinGroupResult -> describeGroupResult

      whenReady(prg.value) { result =>
        val (joinGroupResult, describeGroupResult) = result.right.value

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
        userData = ByteVector("more-test".getBytes)
      )

      val consumerProtocolMetadata = ConsumerProtocol(0, Seq("test"), ByteVector("test".getBytes))

      val prg = for {
        joinGroupResult <- fromEither(client.joinGroup(groupId, None, "consumer", Seq(GroupProtocol("roundrobin", Seq(consumerProtocolMetadata)))))
        describeGroupResult <- pure(client.describeGroups(Set(groupId)))
      } yield joinGroupResult -> describeGroupResult

      whenReady(prg.value) { result =>
        val (joinGroupResult, describeGroupResult) = result.right.value

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

    "apiVersions" in {
      whenReady(client.apiVersions) { result =>
        result should be(right)
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
    val bootstrapBrokers = s"""{ host: "localhost", port: $kafka1Port }"""
    val bootstrapBrokersString = s"flumina.bootstrap-brokers = [$bootstrapBrokers]"
    val kafkaConfig = ConfigFactory.parseString(bootstrapBrokersString)

    ActorSystem("default", kafkaConfig.withFallback(ConfigFactory.load()))
  }

  private lazy val client = KafkaClient()(system, system.dispatcher)

  def run[A](dsl: Free[KafkaA, A]) = client.run(dsl)

  trait KafkaScope {
    lazy val groupId = s"group${System.nanoTime().toString}"
    lazy val topic = s"test${System.nanoTime().toString}"
  }

  override implicit def patienceConfig = PatienceConfig(timeout = 60.seconds, interval = 10.milliseconds)

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  def fromEither[A](f: Future[Either[KafkaResult, A]]) = EitherT(f)

  def pure[A](a: Future[A]) = EitherT.liftT[Future, KafkaResult, A](a)
}
