package vectos.kafka.akkaimpl

import akka.actor.ActorRef
import akka.pattern.ask
import akka.stream.scaladsl.Flow
import akka.util.Timeout
import vectos.kafka.types.{KafkaRequest, KafkaResponse, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try


object Kafka {

  final case class TopicPartition(topic: String, partition: Int)
  final case class Context(connection: ActorRef,
                           requestTimeout: Timeout,
                           executionContext: ExecutionContext)

  def produceSlidingFlow(buffer: Int = 100, parallelism: Int = 20)
                        (implicit ctx: Context) = {
    Flow[(TopicPartition, (Array[Byte], Array[Byte]))]
      .groupBy(maxSubstreams = 100, { case (tp, _) => tp } )
      .sliding(buffer)
      .mergeSubstreams
      .mapAsync(parallelism)(s =>
        produce(s.foldLeft(Map.empty[TopicPartition, List[(Array[Byte], Array[Byte])]]) { case (acc, (tp, keyValue)) =>
          acc.updatedValue(tp, List.empty)(_ ++ List(keyValue))
        })
      )
  }

  def produceBatchFlow(buffer: Int = 100, parallelism: Int = 10)
                      (implicit ctx: Context) = {
    Flow[(TopicPartition, (Array[Byte], Array[Byte]))]
      .groupBy(100, { case (tp, _) => tp } )
      .batch(buffer, { case (tp, keyValue) => Map(tp -> List(keyValue)) }) { case (acc, (tp, keyValue)) =>
        acc.updatedValue(tp, List.empty)(_ ++ List(keyValue))
      }
      .mergeSubstreams
      .mapAsync(parallelism)(produce)
  }

  def listOffsets(implicit ctx: Context) = {

    val topics = Vector(ListOffsetTopicRequest(topic = "test", partitions = Vector(ListOffsetTopicPartitionRequest(partition = 0, time = -1, maxNumberOfOffsets = 1))))
    val request = KafkaRequest.ListOffset(replicaId = -1, topics = topics)

    doRequest(request)
  }

  def metadata(topics: Vector[String])(implicit ctx: Context) = doRequest(KafkaRequest.Metadata(topics))

  def groupCoordinator(groupId: String)(implicit ctx: Context) = doRequest(KafkaRequest.GroupCoordinator(groupId))


  def produce(values: Map[TopicPartition, List[(Array[Byte], Array[Byte])]])
             (implicit ctx: Context) = {
    val topics = values
        .groupBy { case (tp, _) => tp.topic }
          .map { case (topic, tpvalues) =>
            ProduceTopicRequest(topic, tpvalues.map { case (tp, keyValues) =>
              val messages = keyValues.map { case (key, value) =>
                MessageSetEntry(offset = 0, message = Message(magicByte = 0, attributes = 0, key = key.toVector, value = value.toVector))
              }

              ProduceTopicPartitionRequest(tp.partition, messages.toVector) }.toVector)
          }
          .toVector

    val request = KafkaRequest.Produce(acks = 1, timeout = 20000, topics = topics)

    doRequest(request)
//      .flatMap {
//        case u: KafkaResponse.Produce => Future.successful(u)
//        case _ => Future.failed(new Exception("Wrong response type"))
//      }
  }

  def fetch(topicPartitionOffsets: Map[TopicPartition, Int])
           (implicit ctx: Context) = {
    val topics = topicPartitionOffsets
      .groupBy { case (tp, _) => tp.topic }
      .map { case (topic, tpo) =>
        FetchTopicRequest(topic, tpo.map { case (tp, offset) => FetchTopicPartitionRequest(tp.partition, offset, 8 * 1024) }.toVector)
      }
      .toVector

    val request = KafkaRequest.Fetch(
      replicaId = -1,
      maxWaitTime = 500,
      minBytes = 1,
      topics = topics
    )

    doRequest(request)
//      .flatMap {
//        case u: KafkaResponse.Fetch => Future.successful(u)
//        case _ => Future.failed(new Exception("Wrong response type"))
//      }
  }


  private def doRequest(req: KafkaRequest)
                       (implicit ctx: Context) =
    ctx.connection.ask(req)(ctx.requestTimeout).mapTo[Try[KafkaResponse]].flatMap(Future.fromTry)(ctx.executionContext)
}

