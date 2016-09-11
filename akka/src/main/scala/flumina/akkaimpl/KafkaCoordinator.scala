package flumina.akkaimpl

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Status}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import cats.data.Xor
import cats.implicits._
import cats.kernel.Monoid
import flumina.core.ir._
import flumina.core.v090.V090
import flumina.core.{KafkaResult, kafka, splitter}
import scodec.bits.BitVector

import scala.concurrent.Future
import scala.util.control.NonFatal

final class KafkaCoordinator private (settings: KafkaSettings) extends Actor with ActorLogging with Stash {

  import context.dispatcher

  private implicit val timeout: Timeout = Timeout(settings.requestTimeout) //TODO: is this the right place for this??

  private val connectionPool = context.actorOf(KafkaConnectionPool.props(settings.bootstrapBrokers, settings.connectionsPerBroker))
  private val defaultContext = KafkaContext(broker = KafkaBroker.AnyNode, settings = settings.operationalSettings)
  private val interpreter = new V090[Future](x => connectionPool.ask(x).mapTo[BitVector])

  def splitRun[L[_], I, O](s: splitter.RequestSplitter[L, I])(brokerMap: Map[TopicPartition, KafkaBroker], initialRequest: L[I])(runner: L[I] => kafka.Dsl[TopicPartitionResults[O]]): Future[TopicPartitionResults[O]] = {

    def rerun(retries: Int, originalRequest: L[I], resp: TopicPartitionResults[O]) = for {
      newBrokerMap <- syncMetadataInfo(resp.canBeRetried.map(_.topic))
      _ <- FutureUtils.delay(defaultContext.settings.retryBackoff)
      retryResults <- Monoid.combineAll {
        s.split(newBrokerMap, s.canRetry(originalRequest, resp.canBeRetried))
          .map { case (newBroker, newRequest) => Future.successful(resp.resultsExceptWhichCanBeRetried) |+| run(retries = retries + 1)(broker = newBroker, request = newRequest) }
      }
    } yield retryResults

    def run(retries: Int)(broker: KafkaBroker, request: L[I]): Future[TopicPartitionResults[O]] = {
      for {
        resp <- runWithBroker(runner(request), broker)
        res <- if (resp.canBeRetried.nonEmpty && retries < defaultContext.settings.retryMaxCount) {
          log.info(s"errors: ${resp.errors}")
          rerun(retries, request, resp)
        } else {
          Future.successful(resp)
        }
      } yield res
    }

    Monoid.combineAll(s.split(brokerMap, initialRequest).map { case (broker, values) => run(0)(broker, values) })
  }

  def receive = running(State(Map(), Map(), Map(), Map()))

  def syncMetadataInfo(ids: Set[String]) = self.ask(SyncMetadata(ids)).mapTo[Map[TopicPartition, KafkaBroker]]
  def syncGroupInfo(groupId: String) = self.ask(SyncGroup(groupId)).mapTo[Map[String, KafkaBroker]]

  def getBrokerByGroupId(brokerMap: Map[String, KafkaBroker], groupId: String) = brokerMap.get(groupId) match {
    case Some(broker) => Future.successful(broker)
    case None         => syncGroupInfo(groupId).map(x => x.getOrElse(groupId, KafkaBroker.AnyNode))
  }

  def runXorCall[A](broker: KafkaBroker, dsl: kafka.Dsl[KafkaResult Xor A]) = {
    def run(tries: Int, lastError: KafkaResult): Future[KafkaResult Xor A] = {
      if (tries < defaultContext.settings.retryMaxCount) {
        for {
          result <- runWithBroker(dsl, broker)
          next <- result match {
            case Xor.Left(err) if KafkaResult.canRetry(err) =>
              FutureUtils.delay(defaultContext.settings.retryBackoff) flatMap (_ => run(tries + 1, err))
            case Xor.Left(err) =>
              Future.successful(Xor.left(err))
            case Xor.Right(res) =>
              Future.successful(Xor.right(res))
          }
        } yield next
      } else {
        Future.successful(Xor.left(lastError))
      }
    }

    run(0, KafkaResult.NoError)
  }

  def runGroupXorCall[A](groupId: String, brokerMap: Map[String, KafkaBroker], dsl: kafka.Dsl[KafkaResult Xor A]) =
    for {
      broker <- getBrokerByGroupId(brokerMap, groupId)
      result <- runXorCall(broker, dsl)
    } yield result

  def running(state: State): Receive = {

    case KafkaCoordinator.JoinGroup(groupId, memberId, protocol, protocols) =>
      runGroupXorCall(groupId, state.byGroupId, kafka.joinGroup(groupId, memberId, protocol, protocols)) pipeTo sender()

    case KafkaCoordinator.SynchronizeGroup(groupId, generationId, memberId, assignments) =>
      runGroupXorCall(groupId, state.byGroupId, kafka.syncGroup(groupId, generationId, memberId, assignments)) pipeTo sender()

    case KafkaCoordinator.Heartbeat(groupId, generationId, memberId) =>
      runGroupXorCall(groupId, state.byGroupId, kafka.heartbeat(groupId, generationId, memberId)) pipeTo sender()

    case KafkaCoordinator.OffsetsFetch(groupId, values) =>
      (for {
        broker <- getBrokerByGroupId(state.byGroupId, groupId)
        result <- runWithBroker(kafka.offsetFetch(groupId, values), broker)
      } yield result) pipeTo sender()

    case KafkaCoordinator.OffsetsCommit(groupId, generationId, memberId, offsets) =>
      (for {
        broker <- getBrokerByGroupId(state.byGroupId, groupId)
        result <- runWithBroker(kafka.offsetCommit(groupId, generationId, memberId, offsets), broker)
      } yield result) pipeTo sender()

    case KafkaCoordinator.Fetch(values) =>
      splitRun(splitter.Fetch)(state.byTopicPartition, values)(kafka.fetch) pipeTo sender()

    case KafkaCoordinator.Produce(values) =>
      splitRun(splitter.Produce)(state.byTopicPartition, values)(kafka.produce) pipeTo sender()

    case KafkaCoordinator.ListGroups =>
      (for {
        metadata <- runOnAnyBroker(kafka.metadata(Set.empty))
        groups <- Monoid.combineAll(metadata.brokers.map(b => runXorCall(KafkaBroker.Node(b.host, b.port), kafka.listGroups)))
      } yield groups) pipeTo sender()

    case KafkaCoordinator.Metadata(topics) =>
      runMetadata(0, topics) pipeTo sender()

    case KafkaCoordinator.GroupCoordinator(groupId) =>
      runXorCall(KafkaBroker.AnyNode, kafka.groupCoordinator(groupId)) pipeTo sender()

    case KafkaCoordinator.LeaveGroup(groupId, memberId) =>
      runGroupXorCall(groupId, state.byGroupId, kafka.leaveGroup(groupId, memberId)) pipeTo sender()

    case SyncMetadata(topicsToSync) =>
      if (topicsToSync == state.topicsInSync.keySet) {
        log.info("Concurrent sync metadata request, but we are already on it! Adding to subscriber list.")
        context.become(running(state.copy(topicsInSync = state.topicsInSync |+| topicsToSync.map(topic => topic -> List(sender())).toMap)))
      } else {
        val topics = topicsToSync -- state.topicsInSync.keySet
        syncMetadata(topics)
        context.become(running(state.copy(topicsInSync = state.topicsInSync |+| topics.map(topic => topic -> List(sender())).toMap)))
      }

    case SyncGroup(group) =>
      log.info(s"syncing group: $group")
      if (state.groupsInSync.keySet.contains(group)) {
        log.info("Concurrent sync group request, but we are already on it! Adding to subscriber list.")
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

    case Status.Failure(err) => log.error(err, "Error occurred")
  }

  def runMetadata(retries: Int, topicsToSync: Set[String]): Future[Metadata] = {
    for {
      result <- runOnAnyBroker(kafka.metadata(topicsToSync))
      next <- if (result.topicsWhichCanBeRetried.nonEmpty && retries < defaultContext.settings.retryMaxCount) {
        Future.successful(result.withoutRetryErrors) |+| (FutureUtils.delay(defaultContext.settings.retryBackoff) flatMap (_ => runMetadata(retries + 1, result.topicsWhichCanBeRetried)))
      } else {
        Future.successful(result)
      }
    } yield next
  }

  def syncMetadata(topics: Set[String]): Future[MetadataSyncResult] = {

    def toBrokerMap(metadata: Metadata): Map[TopicPartition, KafkaBroker] = (for {
      topics <- metadata.topics
      map = topics.onlyRight match {
        case None => Map.empty[TopicPartition, KafkaBroker]
        case Some(topicInfos) => (for {
          topicInfo <- topicInfos
          broker <- metadata.brokers.find(b => b.nodeId == topicInfo.result.leader).toList
        } yield topicInfo.topicPartition -> KafkaBroker.Node(broker.host, broker.port)).toMap
      }
    } yield map) getOrElse Map.empty[TopicPartition, KafkaBroker]

    runMetadata(0, topics)
      .map(metadata => MetadataSyncResult.Success(toBrokerMap(metadata)))
      .recoverWith {
        case NonFatal(ex) => Future.successful[MetadataSyncResult](MetadataSyncResult.Failed(topics, ex))
      }
      .pipeTo(self)
  }

  def syncGroup(groupId: String) =
    runXorCall(KafkaBroker.AnyNode, kafka.groupCoordinator(groupId))
      .map(x => x.fold[GroupSyncResult](err => GroupSyncResult.Failed(groupId, err), broker => GroupSyncResult.Success(Map(groupId -> KafkaBroker.Node(broker.host, broker.port)))))
      .pipeTo(self)

  def runOnAnyBroker[A](dsl: kafka.Dsl[A]) = run(dsl, defaultContext)
  def runWithBroker[A](dsl: kafka.Dsl[A], broker: KafkaBroker) = run(dsl, defaultContext.copy(broker = broker))
  def run[A](dsl: kafka.Dsl[A], brokerContext: KafkaContext) = dsl(interpreter).run(brokerContext)

  sealed trait MetadataSyncResult
  object MetadataSyncResult {
    sealed case class Failed(topics: Set[String], error: Throwable) extends MetadataSyncResult
    sealed case class Success(brokerMap: Map[TopicPartition, KafkaBroker]) extends MetadataSyncResult
  }

  sealed trait GroupSyncResult
  object GroupSyncResult {
    sealed case class Failed(groupId: String, kafkaError: KafkaResult) extends GroupSyncResult
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
  protected[akkaimpl] final case class OffsetsFetch(groupId: String, values: Set[TopicPartition])
  protected[akkaimpl] final case class OffsetsCommit(groupId: String, generationId: Int, memberId: String, offsets: Map[TopicPartition, OffsetMetadata])
  protected[akkaimpl] final case class Produce(values: List[(TopicPartition, Record)])
  protected[akkaimpl] final case class JoinGroup(groupId: String, memberId: Option[String], protocol: String, protocols: Seq[GroupProtocol])
  protected[akkaimpl] final case class SynchronizeGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment])
  protected[akkaimpl] final case class Heartbeat(groupId: String, generationId: Int, memberId: String)
  protected[akkaimpl] final case class Fetch(values: Map[TopicPartition, Long])
  protected[akkaimpl] final case class LeaveGroup(groupId: String, memberId: String)
  protected[akkaimpl] final case class GroupCoordinator(groupId: String)
  protected[akkaimpl] final case class Metadata(topics: Set[String])
  protected[akkaimpl] final case object ListGroups

  def props(settings: KafkaSettings) = Props(new KafkaCoordinator(settings))
}