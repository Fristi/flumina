package flumina.akkaimpl

import akka.NotUsed
import akka.actor.{ActorRefFactory, ActorSystem}
import akka.stream.scaladsl.Source
import akka.pattern.ask
import akka.util.Timeout
import flumina.types.ir.{OffsetMetadata, Record, RecordEntry, TopicPartition}
import scala.concurrent.duration._

final class KafkaClient private (settings: KafkaSettings, actorRefFactory: ActorRefFactory) {

  private implicit val timeout: Timeout = Timeout(settings.requestTimeout)

  private val coordinator = actorRefFactory.actorOf(KafkaCoordinator.props(settings))

  def produce(values: Map[TopicPartition, List[Record]]) = (coordinator ? Produce(values)).mapTo[Result[Long]]
  def singleFetch(values: Map[TopicPartition, Long]) =
    (coordinator ? Fetch(values)).mapTo[Result[List[RecordEntry]]]

  def fetchFromBeginning(topicPartitions: Set[TopicPartition]): Source[RecordEntry, NotUsed] = {
    def run(tried: Int, lastResult: Result[List[RecordEntry]], lastOffsetRequest: Map[TopicPartition, Long]): Source[RecordEntry, NotUsed] = {
      if (lastResult.errors.nonEmpty) {
        Source.failed(new Exception("Failing..."))
      } else {
        val newOffsetRequest = lastResult.success
          .map(x => x.topicPartition -> (if (x.value.isEmpty) 0l else x.value.maxBy(y => y.offset).offset))
          .toMap

        if (newOffsetRequest === lastOffsetRequest) {
          Source.fromFuture(FutureUtils.delay(1.seconds * tried.toLong))
            .flatMapConcat(_ => run(tried + 1, lastResult, lastOffsetRequest))
        } else {
          Source(lastResult.success.flatMap(_.value)) ++ Source.fromFuture(singleFetch(newOffsetRequest))
            .flatMapConcat(r => run(0, r, newOffsetRequest))
        }
      }
    }

    val firstRequest = topicPartitions.map(_ -> 0l).toMap

    Source.fromFuture(singleFetch(firstRequest))
      .flatMapConcat(r => run(0, r, firstRequest))
  }

  def offsetsFetch(groupId: String, values: Set[TopicPartition]) = (coordinator ? OffsetsFetch(groupId, values)).mapTo[Result[OffsetMetadata]]
  def offsetsCommit(groupId: String, offsets: Map[TopicPartition, OffsetMetadata]) = (coordinator ? OffsetsCommit(groupId, offsets)).mapTo[Result[Unit]]
}

object KafkaClient {
  def apply(settings: KafkaSettings)(implicit system: ActorSystem) =
    new KafkaClient(settings, system)
}