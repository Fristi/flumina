package flumina

import _root_.monix.execution._
import _root_.monix.reactive._
import flumina.akkaimpl.KafkaClient
import flumina.core.ir._

import scala.concurrent.ExecutionContext
import scala.math.Integral

package object monix {

  implicit class RichKafkaClient(val kafkaClient: KafkaClient) {

    def produce[R, N: Integral](topic: String, nrPartitions: Int, buffer: OverflowStrategy.Synchronous[R], toRecord: R => Record, partition: R => N, compression: Compression)(implicit EC: ExecutionContext): Consumer[R, Unit] = {
      PartitionConsumer.partitionConsume[R, N](nrPartitions, buffer, partition) {
        case (part, elems) =>
          kafkaClient.produceN(compression, elems.map(r => TopicPartitionValue(TopicPartition(topic, part), toRecord(r)))) flatMap {
            case TopicPartitionValues(errs, _) if errs.nonEmpty => Ack.Stop
            case _                                              => Ack.Continue
          }
      }
    }

    def consume(topic: String, consumptionStrategy: ConsumptionStrategy, offsetStore: OffsetStore)(implicit S: Scheduler): Observable[TopicPartitionValue[RecordEntry]] = {

      def offsets(metadata: Metadata, offsets: Vector[TopicPartitionValue[Long]]): Observable[TopicPartitionValue[RecordEntry]] = {
        val brokerOffsets = (0 until metadata.topics.size).map { s =>
          val topicPartition = TopicPartition(topic, s)
          val offset = offsets.find(_.topicPartition == topicPartition).map(_.result).getOrElse(0l)

          TopicPartitionValue(topicPartition, offset)
        }
        val sources = brokerOffsets.map(tp => new TopicConsumer(kafkaClient, tp, consumptionStrategy, offsetStore))

        Observable.merge(sources: _*)
      }

      for {
        currentOffsets <- Observable.fromTask(offsetStore.load(topic))
        metadata <- Observable.fromFuture(kafkaClient.metadata(Set(topic)))
        entry <- offsets(metadata, currentOffsets)
      } yield entry
    }

  }
}

