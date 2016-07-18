package vectos.kafka.akkaimpl

import cats.syntax.all._
import cats.scalatest._
import org.scalatest.Matchers
import vectos.kafka.types.ir.MessageEntry
import vectos.kafka.types.kafka

abstract class KafkaProtocolTest extends KafkaProtocolTestStack with Matchers with XorMatchers with XorValues {
  s"Kafka: version protocol $protocolVersion" should {
    "metadata" in new KafkaScope {
      whenReady(run(kafka.metadata(Vector.empty))) { result =>
        result should be(right)
      }
    }
    //    "offsets" in new KafkaScope {
    //      val group = randomGroup
    //      val prg = for {
    //        produceResult <- kafka.produce(Map(topicPartition -> List("key".getBytes -> "Hello world".getBytes)))
    //        groupCoordinator <- kafka.groupCoordinator(group)
    //        _ <- kafka.joinGroup(group, "consumer", Seq(JoinGroupProtocol("protocol", "test".getBytes.toVector)))
    //        fetchResult <- kafka.fetch(Map(topicPartition -> 0l))
    //        offsetCommitResult <- kafka.offsetCommit(group, Map(topicPartition -> OffsetMetadata(fetchResult.head.value.maxBy(_.offset).offset, Some("metadata"))))
    //        offsetFetchResult <- kafka.offsetFetch(group, Set(topicPartition))
    //      } yield offsetCommitResult -> offsetFetchResult
    //
    //      whenReady(run(prg)) { result =>
    //        val (offsetCommitResult, offsetFetchResult) = result.value
    //
    //      }
    //    }

    "listOffsets" in new KafkaScope {
      whenReady(run(kafka.listOffsets(Set.empty))) { result =>
        result should be(right)
      }
    }
    "listGroups" in new KafkaScope {
      whenReady(run(kafka.listGroups)) { result =>
        result should be(right)
      }
    }
    //    "groupCoordinator" in new KafkaScope {
    //      whenReady(run(kafka.groupCoordinator("test"))) { result =>
    //        result should be(right)
    //      }
    //    }
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
  }

}
