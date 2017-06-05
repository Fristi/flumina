package flumina

import _root_.monix.execution._
import _root_.monix.reactive._
import cats.Monad
import cats.implicits._
import flumina.client.KafkaClient
import scodec.Attempt

import scala.concurrent.ExecutionContext

package object monix {

  implicit class RichKafkaClient(val kafkaClient: KafkaClient) {

    def produce[R](
      topic:             String,
      nrPartitions:      Int,
      codecErrorHandler: CodecErrorHandler               = CodecErrorHandler.stop,
      buffer:            OverflowStrategy.Synchronous[R] = OverflowStrategy.Unbounded,
      compression:       Compression                     = Compression.Snappy)(implicit EC: ExecutionContext, P: KafkaPartitioner[R], E: KafkaEncoder[R]): Consumer[R, Unit] = {
      PartitionConsumer.partitionConsume[R](nrPartitions, buffer, { case (nrPartitions, value) => P.selectPartition(value, nrPartitions) }) {
        case (part, elems) =>
          elems.toList.traverse[Attempt, TopicPartitionValue[Record]](r => E.encode(r).map(rr => TopicPartitionValue(TopicPartition(topic, part), rr))) match {
            case Attempt.Successful(records) =>
              kafkaClient.produceN(compression, records).flatMap {
                case TopicPartitionValues(errs, _) if errs.nonEmpty => Ack.Stop
                case _                                              => Ack.Continue
              }

            case Attempt.Failure(err) => codecErrorHandler.handle(err)
          }
      }
    }

    def messages[A](
      topic:               String,
      consumptionStrategy: ConsumptionStrategy,
      codecErrorHandler:   CodecErrorHandler   = CodecErrorHandler.stop)(implicit S: Scheduler, D: KafkaDecoder[A]): Observable[TopicPartitionValue[OffsetValue[A]]] = {

      def sources(metadata: Metadata): Observable[TopicPartitionValue[OffsetValue[A]]] = {
        Observable.merge(
          metadata.topics.toSeq.map(tp => new TopicConsumer(kafkaClient, tp.topicPartition, 0l, consumptionStrategy, codecErrorHandler)): _*)
      }

      for {
        metadata <- Observable.fromFuture(kafkaClient.metadata(Set(topic)))
        entry <- sources(metadata)
      } yield entry
    }
  }

  implicit val monadAttempt: Monad[Attempt] = new Monad[Attempt] {
    override def flatMap[A, B](fa: Attempt[A])(f: (A) => Attempt[B]): Attempt[B] = fa.flatMap(f)

    override def tailRecM[A, B](a: A)(f: (A) => Attempt[Either[A, B]]): Attempt[B] = f(a).flatMap {
      case Left(v)  => tailRecM(v)(f)
      case Right(v) => pure(v)
    }

    override def pure[A](x: A): Attempt[A] = Attempt.successful(x)
  }

}

