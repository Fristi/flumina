package flumina.akkaimpl

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Status}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import cats.data.Xor
import cats.implicits._
import flumina.akkaimpl.versions.V0
import flumina.types.ir.{KafkaError, Metadata, OffsetMetadata, Record, TopicPartition, TopicPartitionResult}
import flumina.types.{KafkaResult, kafka}

import scala.concurrent.Future

final class KafkaCoordinator private (settings: KafkaSettings) extends Actor with ActorLogging with Stash {

  import context.dispatcher

  private implicit val timeout: Timeout = Timeout(settings.requestTimeout) //TODO: is this the right place for this??

  private val defaultContext = KafkaContext(
    connectionPool = context.actorOf(KafkaConnectionPool.props(settings.bootstrapBrokers, settings.connectionsPerBroker)),
    broker = KafkaBroker.AnyNode,
    requestTimeout = Timeout(settings.requestTimeout),
    settings = settings.operationalSettings
  )

  abstract class RequestSplitter[V, R, BM] {
    def split(brokerMap: Map[BM, KafkaBroker], values: V): Map[KafkaBroker, V]
    def canRetry(values: V, results: List[TopicPartitionResult[R]]): V
    def topicPartitions(values: V): Set[TopicPartition]
    def brokerMapIds(values: V): Set[String]
    def requestNewBrokerMap(ids: Set[String]): Future[Map[BM, KafkaBroker]]
    def run(values: V, broker: KafkaBroker): Future[KafkaError Xor List[TopicPartitionResult[R]]]
  }

  abstract class GroupRequestSplitter[V, R](groupId: String) extends RequestSplitter[V, R, String] {

    def split(brokerMap: Map[String, KafkaBroker], values: V) =
      Map(brokerMap.getOrElse(groupId, defaultContext.broker) -> values)

    def requestNewBrokerMap(ids: Set[String]) =
      self.ask(SyncGroup(groupId)).mapTo[Map[String, KafkaBroker]]

    def brokerMapIds(values: V): Set[String] =
      Set(groupId)
  }

  final class TopicPartitionRequestSplitter[Req, Resp](f: (Map[TopicPartition, Req], KafkaBroker) => Future[KafkaError Xor List[TopicPartitionResult[Resp]]]) extends RequestSplitter[Map[TopicPartition, Req], Resp, TopicPartition] {
    def split(brokerMap: Map[TopicPartition, KafkaBroker], values: Map[TopicPartition, Req]) =
      values
        .map { case (tp, msgs) => (brokerMap.getOrElse(tp, defaultContext.broker), tp, msgs) }
        .groupBy { case (broker, _, _) => broker }
        .map { case (broker, topicPartitionMessages) => broker -> topicPartitionMessages.map(x => x._2 -> x._3).toMap }

    def run(values: Map[TopicPartition, Req], broker: KafkaBroker) =
      f(values, broker)

    def canRetry(values: Map[TopicPartition, Req], results: List[TopicPartitionResult[Resp]]) =
      (for {
        result <- results.filter(x => KafkaResult.canRetry(x.kafkaResult))
        //TODO: is this something we could improve upon ? No error thrown here, but maybe it should???
        messages <- values.get(result.topicPartition).toList
      } yield result.topicPartition -> messages).toMap

    def topicPartitions(values: Map[TopicPartition, Req]) =
      values.keySet

    def requestNewBrokerMap(ids: Set[String]) =
      self.ask(SyncMetadata(ids)).mapTo[Map[TopicPartition, KafkaBroker]]

    def brokerMapIds(values: Map[TopicPartition, Req]) =
      values.keySet.map(_.topic)
  }

  def commitOffsetSplitter(groupId: String) = new GroupRequestSplitter[Map[TopicPartition, OffsetMetadata], Unit](groupId) {
    def canRetry(values: Map[TopicPartition, OffsetMetadata], results: List[TopicPartitionResult[Unit]]): Map[TopicPartition, OffsetMetadata] =
      (for {
        result <- results.filter(x => KafkaResult.canRetry(x.kafkaResult))
        offsetMetadata <- values.get(result.topicPartition).toList
      } yield result.topicPartition -> offsetMetadata).toMap

    def topicPartitions(values: Map[TopicPartition, OffsetMetadata]): Set[TopicPartition] = values.keySet

    def run(values: Map[TopicPartition, OffsetMetadata], broker: KafkaBroker) =
      runWithBroker(kafka.offsetCommit(groupId, values), broker)
  }

  def fetchOffsetSplitter(groupId: String) = new GroupRequestSplitter[Set[TopicPartition], OffsetMetadata](groupId) {

    def canRetry(values: Set[TopicPartition], results: List[TopicPartitionResult[OffsetMetadata]]) =
      results.filter(x => KafkaResult.canRetry(x.kafkaResult))
        .map(_.topicPartition)
        .toSet

    def topicPartitions(values: Set[TopicPartition]) = values

    def run(values: Set[TopicPartition], broker: KafkaBroker) =
      runWithBroker(kafka.offsetFetch(groupId, values), broker)
  }

  val fetchRequestSplitter =
    new TopicPartitionRequestSplitter((values: Map[TopicPartition, Long], broker: KafkaBroker) => runWithBroker(kafka.fetch(values), broker))

  val produceRequestSplitter =
    new TopicPartitionRequestSplitter((values: Map[TopicPartition, List[Record]], broker: KafkaBroker) => runWithBroker(kafka.produce(values), broker))

  def splitRun[V, R, BM](splitter: RequestSplitter[V, R, BM])(brokerMap: Map[BM, KafkaBroker], initialRequest: V)(implicit H: HasSize[V]) = {

    def run(retries: Int, broker: KafkaBroker, request: V, results: Result[R]): Future[Result[R]] = {
      log.debug(s"Split run -> Try $retries [broker: $broker]")

      for {
        resp <- splitter.run(request, broker)
        res <- resp match {
          case Xor.Left(err) =>
            val topicPartitionErrors = splitter.topicPartitions(request)
            log.error(s"Error ($err) for $topicPartitionErrors")
            Future.successful(results |+| Result(Set.empty, topicPartitionErrors))

          case Xor.Right(topicPartitionResults) =>

            log.debug(s"Results from request $retries: ${topicPartitionResults.map(x => x.topicPartition -> x.kafkaResult)} [success: ${results.success.map(x => x.topicPartition -> x.kafkaResult)}, error: ${results.errors}]")

            val canRetry = splitter.canRetry(request, topicPartitionResults)
            def errorsWithoutRetries = topicPartitionResults.filter(x => x.kafkaResult != KafkaResult.NoError && !KafkaResult.canRetry(x.kafkaResult))
            def errors = topicPartitionResults.filter(x => x.kafkaResult != KafkaResult.NoError)
            def success = topicPartitionResults.filter(x => x.kafkaResult == KafkaResult.NoError)
            def newResultsWithoutRetries = results |+| Result(success.toSet, errorsWithoutRetries.map(_.topicPartition).toSet)
            def newResultsWithCurrentErrors = Future.successful(results |+| Result(success.toSet, errors.map(_.topicPartition).toSet))

            if (H.nonEmpty(canRetry) && retries < defaultContext.settings.retryMaxCount) {
              log.error(s"We encountered several requests which can be retried: ${H.size(canRetry)}")

              val rerun = for {

                newBrokerMap <- splitter.requestNewBrokerMap(splitter.brokerMapIds(request))

                _ <- FutureUtils.delay(defaultContext.settings.retryBackoff)

                retryResults <- Future.sequence {
                  splitter
                    .split(newBrokerMap, canRetry)
                    .map { case (newBroker, newRequest) => run(retries = retries + 1, broker = newBroker, request = newRequest, results = newResultsWithoutRetries) }
                }

              } yield retryResults.foldLeft(Result.zero[R])(_ |+| _)

              //if the rerun fails, due a timeout for example we can fallback to a error case
              rerun fallbackTo newResultsWithCurrentErrors
            } else {
              newResultsWithCurrentErrors
            }
        }
      } yield res
    }

    Future.sequence {
      splitter.split(brokerMap, initialRequest)
        .map { case (broker, newRequest) => run(retries = 0, broker = broker, request = newRequest, results = Result.zero) }
    }.map(x => x.foldLeft(Result.zero[R])(_ |+| _))
  }

  def receive = running(State(Map(), Map(), Map(), Map()))

  def running(state: State): Receive = {

    case OffsetsFetch(groupId, values) =>
      splitRun(fetchOffsetSplitter(groupId))(brokerMap = state.byGroupId, initialRequest = values) pipeTo sender()

    case OffsetsCommit(groupId, offsets) =>
      splitRun(commitOffsetSplitter(groupId))(brokerMap = state.byGroupId, initialRequest = offsets) pipeTo sender()

    case Fetch(values) =>
      splitRun(fetchRequestSplitter)(brokerMap = state.byTopicPartition, initialRequest = values) pipeTo sender()

    case Produce(values) =>
      splitRun(produceRequestSplitter)(brokerMap = state.byTopicPartition, initialRequest = values) pipeTo sender()

    case SyncMetadata(topicsToSync) =>
      if (topicsToSync == state.topicsInSync.keySet) {
        log.info("Concurrent sync request, but we are already on it! Adding to subscriber list.")
        context.become(running(state.copy(topicsInSync = state.topicsInSync |+| topicsToSync.map(topic => topic -> List(sender())).toMap)))
      } else {
        val topics = topicsToSync -- state.topicsInSync.keySet
        syncMetadata(topics)
        context.become(running(state.copy(topicsInSync = state.topicsInSync |+| topics.map(topic => topic -> List(sender())).toMap)))
      }

    case SyncGroup(group) =>
      if (state.groupsInSync.keySet.contains(group)) {
        log.info("Concurrent sync request, but we are already on it! Adding to subscriber list.")
      } else {
        syncGroup(group)
      }

      context.become(running(state.copy(groupsInSync = state.groupsInSync |+| Map(group -> List(sender())))))

    case GroupSyncResult.Success(brokerMap) =>
      log.debug(s"Syncing group success: $brokerMap")
      val updateGroups = brokerMap.keySet
      val newState = state.byGroupId ++ brokerMap

      for (updateGroups <- updateGroups) {
        state.groupsInSync
          .get(updateGroups)
          .foreach(refs => refs.foreach(_ ! newState))
      }

      context.become(running(state.copy(groupsInSync = state.groupsInSync -- updateGroups, byGroupId = newState)))

    case MetadataSyncResult.Success(brokerMap) =>
      log.debug(s"Syncing metadata success: $brokerMap")
      val updatedTopics = brokerMap.keySet.map(_.topic)
      val newState = state.byTopicPartition ++ brokerMap

      for (updatedTopic <- updatedTopics) {
        state.topicsInSync
          .get(updatedTopic)
          .foreach(refs => refs.foreach(_ ! newState))
      }

      context.become(running(state.copy(topicsInSync = state.topicsInSync -- updatedTopics, byTopicPartition = newState)))

    case MetadataSyncResult.Failed(topics, error) =>
      log.error(s"MetadataSyncResult.Failed for $topics: $error")

      for (topic <- topics) {
        state.topicsInSync
          .get(topic)
          .foreach(refs => refs.foreach(_ ! Status.Failure(new Exception(s"Failed to sync topics: $topics"))))
      }
      context.become(running(state.copy(topicsInSync = state.topicsInSync -- topics)))

    case GroupSyncResult.Failed(groupId, error) =>
      log.error(s"GroupSyncResult.Failed for $groupId: $error")

      state.groupsInSync
        .get(groupId)
        .foreach(refs => refs.foreach(_ ! Status.Failure(new Exception(s"Failed to sync groupId: $groupId"))))

      context.become(running(state.copy(groupsInSync = state.groupsInSync - groupId)))
  }

  def syncMetadata(topics: Set[String]) = {

    def toBrokerMap(metadata: Metadata): Map[TopicPartition, KafkaBroker] = (for {
      topicPartitionResult <- metadata.metadata
      //TODO: is this something we could improve upon ? No error thrown here, but maybe it should???
      broker <- metadata.brokers.find(_.nodeId == topicPartitionResult.value.leader).toList
    } yield topicPartitionResult.topicPartition -> KafkaBroker.Node(broker.host, broker.port)).toMap

    runOnAnyBroker(kafka.metadata(topics))
      .map(x => x.fold[MetadataSyncResult](err => MetadataSyncResult.Failed(topics, err), metadata => MetadataSyncResult.Success(toBrokerMap(metadata))))
      .pipeTo(self)
  }

  def syncGroup(groupId: String) =
    runOnAnyBroker(kafka.groupCoordinator(groupId))
      .map(x => x.fold[GroupSyncResult](err => GroupSyncResult.Failed(groupId, err), broker => GroupSyncResult.Success(Map(groupId -> KafkaBroker.Node(broker.host, broker.port)))))
      .pipeTo(self)

  //TODO: select this automagically
  val interpreter = new V0()

  def runOnAnyBroker[A](dsl: kafka.Dsl[A]) = run(dsl, defaultContext)
  def runWithBroker[A](dsl: kafka.Dsl[A], broker: KafkaBroker) = run(dsl, defaultContext.copy(broker = broker))
  def run[A](dsl: kafka.Dsl[A], brokerContext: KafkaContext) = dsl(interpreter).value.run(brokerContext)

  sealed trait MetadataSyncResult
  object MetadataSyncResult {
    sealed case class Failed(topics: Set[String], kafkaError: KafkaError) extends MetadataSyncResult
    sealed case class Success(brokerMap: Map[TopicPartition, KafkaBroker]) extends MetadataSyncResult
  }

  sealed trait GroupSyncResult
  object GroupSyncResult {
    sealed case class Failed(groupId: String, kafkaError: KafkaError) extends GroupSyncResult
    sealed case class Success(brokerMap: Map[String, KafkaBroker]) extends GroupSyncResult
  }

  private final case class SyncMetadata(topics: Set[String])
  private final case class SyncGroup(group: String)

  private final case class State(
    topicsInSync:     Map[String, List[ActorRef]],
    groupsInSync:     Map[String, List[ActorRef]],
    byTopicPartition: Map[TopicPartition, KafkaBroker],
    byGroupId:        Map[String, KafkaBroker]
  )

}

object KafkaCoordinator {
  def props(settings: KafkaSettings) = Props(new KafkaCoordinator(settings))
}

