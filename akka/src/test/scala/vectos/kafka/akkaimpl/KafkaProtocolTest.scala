package vectos.kafka.akkaimpl

import cats.scalatest._
import cats.syntax.all._
import org.scalatest.Matchers
import vectos.kafka.types.ir._
import vectos.kafka.types.{KafkaResult, kafka}

abstract class KafkaProtocolTest extends KafkaProtocolTestStack with Matchers with XorMatchers with XorValues {
  s"Kafka: version protocol $protocolVersion" should {
    "metadata" in new KafkaScope {
      whenReady(run(kafka.metadata(Vector.empty))) { result =>
        val metadata = result.value

        metadata.brokers should have size 1
        metadata.brokers.head.nodeId shouldBe 0
        metadata.brokers.head.host shouldBe "localhost"
        metadata.brokers.head.port shouldBe 9999
      }
    }
    "listOffsets" in new KafkaScope {
      whenReady(run(kafka.listOffsets(Set.empty))) { result =>
        result.value should have size 0
      }
    }

    "listGroups" in new KafkaScope {
      whenReady(run(kafka.listGroups)) { result =>
        result.value should have size 0
      }
    }
    "groupCoordinator" in new KafkaScope {
      whenReady(run(kafka.groupCoordinator("test"))) { result =>
        val groupCoordinator = result.value

        groupCoordinator.coordinatorId shouldBe 0
        groupCoordinator.coordinatorHost shouldBe "localhost"
        groupCoordinator.coordinatorPort shouldBe 9999
      }
    }
    "produce" in new KafkaScope {
      whenReady(run(kafka.produce(Map(topicPartition -> List("key".getBytes -> "Hello world".getBytes))))) { result =>
        val produceResult = result.value

        produceResult should have size 1
        produceResult.containsError shouldBe false
        produceResult.head.topicPartition shouldBe topicPartition
        produceResult.head.value shouldBe 0
      }
    }

    "fetch" in new KafkaScope {
      val prg = for {
        _ <- kafka.produce(Map(topicPartition -> List("key".getBytes -> "Hello world".getBytes)))
        fetchResult <- kafka.fetch(Map(topicPartition -> 0l))
      } yield fetchResult

      whenReady(run(prg)) { result =>
        val fetchResult = result.value

        fetchResult should have size 1
        fetchResult.containsError shouldBe false
        fetchResult.head.topicPartition shouldBe topicPartition
        fetchResult.head.value should have size 1
        fetchResult.head.value.head shouldBe MessageEntry(0, "key".getBytes, "Hello world".getBytes)
      }
    }

    "joinGroup" in new KafkaScope {
      whenReady(run(kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocolMetadata(0, Vector("test"), Array.empty))))))) { result =>
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
        produceResult <- kafka.produce(Map(topicPartition -> List("key".getBytes -> "Hello world".getBytes, "key".getBytes -> "Hello world".getBytes)))
        joinGroupResult <- kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocolMetadata(0, Vector("test"), Array.empty)))))
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

    "describeGroups" in new KafkaScope {
      val prg = for {
        _ <- kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocolMetadata(0, Seq("test"), Array.empty)))))
        describeGroupResult <- kafka.describeGroups(Set(groupId))
      } yield describeGroupResult

      whenReady(run(prg)) { result =>
        val describeGroupResult = result.value

        describeGroupResult should have size 1
        describeGroupResult.head.errorCode shouldBe KafkaResult.NoError
        describeGroupResult.head.groupId should not be empty
        describeGroupResult.head.state should not be empty
        describeGroupResult.head.protocolType should not be empty
        describeGroupResult.head.members should not be empty
      }
    }

    "leaveGroup" in new KafkaScope {
      val prg = for {
        joinGroupResult <- kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocolMetadata(0, Seq("test"), Array.empty)))))
        leaveGroupResult <- kafka.leaveGroup(groupId, joinGroupResult.memberId)
      } yield leaveGroupResult

      whenReady(run(prg)) { result =>
        result should be(right)
      }
    }

    "heartbeat" in new KafkaScope {
      val prg = for {
        joinGroupResult <- kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocolMetadata(0, Seq("test"), Array.empty)))))
        heartbeatResult <- kafka.heartbeat(groupId, joinGroupResult.generationId, joinGroupResult.memberId)
      } yield heartbeatResult

      whenReady(run(prg)) { result =>
        result should be(left) //TODO: rebalance in progress??,, should it delay??
      }
    }

    "syncGroup" in new KafkaScope {

      val prg = for {
        joinGroupResult <- kafka.joinGroup(groupId, "consumer", Seq(GroupProtocol("roundrobin", Seq(ConsumerProtocolMetadata(0, Seq("test"), "test".getBytes)))))
        syncGroupResult <- kafka.syncGroup(
          groupId = groupId,
          generationId = joinGroupResult.generationId,
          memberId = joinGroupResult.memberId,
          assignments = Seq(
            GroupAssignment(
              memberId = joinGroupResult.memberId,
              memberAssignment = MemberAssignment(
                version = 0,
                topicPartition = Seq(MemberAssignmentTopicPartition(topicName = "test", partitions = Seq(0))),
                userData = "more-test".getBytes
              )
            )
          )
        )
      } yield syncGroupResult

      whenReady(run(prg)) { result =>
        val syncGroupResult = result.value
        syncGroupResult.version shouldBe 0
        syncGroupResult.topicPartition should have size 1
        syncGroupResult.topicPartition.head shouldBe MemberAssignmentTopicPartition("test", Seq(0))
        syncGroupResult.userData shouldBe "more-test".getBytes
      }
    }
  }
}
