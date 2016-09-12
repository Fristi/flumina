package flumina.akkaimpl

import akka.pattern.pipe
import akka.stream.actor.{ActorPublisherMessage => M}
import akka.actor.{Actor, ActorLogging, Props, Status}
import akka.stream.actor.ActorPublisher
import flumina.core.ir._
import flumina.core.{KafkaFailure, KafkaResult}
import flumina.core.KafkaFailure._
import scodec.bits.ByteVector

import scala.concurrent.Future

/**
 * https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.9+Consumer+Rewrite+Design
 */
final class KafkaConsumer private (groupId: String, topicPartitions: Set[TopicPartition], client: KafkaClient, settings: KafkaOperationalSettings)
    extends ActorPublisher[TopicPartitionRecordEntry]
    with ActorLogging {

  import context.dispatcher

  override def preStart = {
    context.system.scheduler.schedule(settings.heartbeatInterval, settings.heartbeatInterval, self, DoHeartbeat)
    join(None)
  }

  def receive = joining

  private def joining: Actor.Receive = {
    case Status.Failure(ex) if isActive => onError(ex)
    case M.Cancel                       => context stop self
    case InitFailed(error)              => onError(new Exception(s"Error occured: $error"))
    case InitSuccess(groupResult, offsets) =>
      performRequestIfNeeded(State(
        memberId = groupResult.memberId,
        generationId = groupResult.generationId,
        nextFetchRequest = offsets,
        buffer = Nil
      ))
  }

  private def requestInProgress(state: State): Receive = {
    case FetchResult(fetchResults, commitResults) =>
      if (commitResults.errors.nonEmpty || fetchResults.errors.nonEmpty) {
        if (commitResults.errors.exists(x => x.result == KafkaResult.RebalanceInProgress || x.result == KafkaResult.IllegalGeneration)) {
          join(Some(state.memberId))
          context.become(joining)
        } else {
          onError(new Exception(s"Something went bad mokay  ${commitResults.errors} || ${fetchResults.errors}"))
        }
      } else {
        processFetchResults(state, fetchResults)
      }

    case Status.Failure(ex) => if (isActive) onError(ex)
    case M.Cancel           => context.become(closing)
    case DoHeartbeat        => heartbeat(state.generationId, state.memberId)
    case HeartbeatFailed(result) if result == KafkaResult.RebalanceInProgress || result == KafkaResult.IllegalGeneration =>
      join(Some(state.memberId))
  }

  private def waitingForDownstream(state: State): Receive = {
    case _: M.Request if isActive && totalDemand > 0 => performRequestIfNeeded(state)
    case M.Cancel                                    => context stop self
    case DoHeartbeat                                 => heartbeat(state.generationId, state.memberId)
    case HeartbeatFailed(result) if result == KafkaResult.RebalanceInProgress || result == KafkaResult.IllegalGeneration =>
      join(Some(state.memberId))
  }

  private def closing: Receive = {
    case FetchResult(commitResults, fetchResults) =>
      if (commitResults.errors.nonEmpty || fetchResults.errors.nonEmpty) {
        onError(new Exception(s"Something went bad mokay  ${commitResults.errors} || ${fetchResults.errors}"))
      } else {
        context stop self
      }
    case Status.Failure(ex) => if (isActive) onError(ex)
  }

  private def processFetchResults(state: State, results: TopicPartitionResults[List[RecordEntry]]) = {
    val newResults = for {
      topicPartitionResult <- results.success
      recordEntry <- topicPartitionResult.result
    } yield TopicPartitionRecordEntry(topicPartitionResult.topicPartition, recordEntry)

    processDownStreamRequests(state, newResults)
  }

  private def processDownStreamRequests(state: State, newResults: Seq[TopicPartitionRecordEntry]) = {

    val offsetsFromResults = newResults
      .groupBy(_.topicPartition)
      .map { case (topicPartition, entries) => topicPartition -> (entries.maxBy(_.recordEntry.offset).recordEntry.offset + 1l) }

    val newFetchRequest = state.nextFetchRequest.combineIfPresent(offsetsFromResults, Math.max)

    val demand = totalDemand.toInt
    val newBuffer = state.buffer ++ newResults
    val newBufferExceptConsumed = newBuffer.drop(demand)
    val topicPartitionRecordEntries = newBuffer.take(demand)
    val commit = topicPartitionRecordEntries
      .groupBy(_.topicPartition)
      .map { case (topicPartition, entries) => topicPartition -> OffsetMetadata(entries.maxBy(_.recordEntry.offset).recordEntry.offset, None) }

    val newState = state.copy(
      buffer = newBufferExceptConsumed,
      nextFetchRequest = newFetchRequest
    )

    topicPartitionRecordEntries.foreach(onNext)

    val fetchReq = if (newState.buffer.isEmpty && isActive && demand > 0) newFetchRequest else Map.empty[TopicPartition, Long]

    if (commit.nonEmpty || fetchReq.nonEmpty) {
      commitAndFetch(newState.generationId, newState.memberId, fetchReq, commit)
      context.become(requestInProgress(newState))
    } else {
      context.become(waitingForDownstream(newState))
    }
  }

  private def commitAndFetch(generationId: Int, memberId: String, fetch: Map[TopicPartition, Long], commit: Map[TopicPartition, OffsetMetadata]) = {
    def fetchReq = if (fetch.isEmpty) Future.successful(TopicPartitionResults.zero[List[RecordEntry]]) else client.fetch(fetch)
    def commitReq = if (commit.isEmpty) Future.successful(TopicPartitionResults.zero[Unit]) else client.offsetCommit(groupId, generationId, memberId, commit)

    (fetchReq zip commitReq)
      .map { case (fetchResults, commitResults) => FetchResult(fetchResults, commitResults) } pipeTo self
  }

  private def performRequestIfNeeded(state: State): Unit = {
    if (isActive && totalDemand > 0) {
      if (state.buffer.isEmpty) {
        commitAndFetch(state.generationId, state.memberId, state.nextFetchRequest, Map())
        context.become(requestInProgress(state))
      } else {
        processDownStreamRequests(state, Seq.empty)
      }
    } else {
      context.become(waitingForDownstream(state))
    }
  }

  private def joinGroup(memberId: Option[String]): KafkaFailure[Future, (JoinGroupResult, Map[TopicPartition, Long])] = for {
    gr <- fromAsyncXor(client.joinGroup(groupId, memberId, "consumer", Seq(GroupProtocol("range", Seq(ConsumerProtocol(0, topicPartitions.map(_.topic).toSeq, ByteVector.empty))))))
    memberAssignment <- if (gr.leaderId == gr.memberId) {
      fromAsyncXor(client.syncGroup(groupId, gr.generationId, gr.memberId, settings.consumeAssignmentStrategy.assign(gr.leaderId, topicPartitions, gr.members)))
    } else {
      fromAsyncXor(client.syncGroup(groupId, gr.generationId, gr.memberId, Seq.empty))
    }
    offsets <- fromAsync(client.offsetFetch(groupId, memberAssignment.topicPartitions.toSet))
  } yield gr -> offsets.success.map(x => x.topicPartition -> (if (x.result.offset == -1) 0l else x.result.offset)).toMap

  def heartbeat(generationId: Int, memberId: String) =
    client.heartbeat(groupId, generationId, memberId).map(_.fold[HeartbeatResult](HeartbeatFailed, _ => HeartbeatSuccess)) pipeTo self

  def join(memberId: Option[String]) = {
    joinGroup(memberId).value.map(_.fold[InitResult](InitFailed, InitSuccess.tupled)) pipeTo self
    context.become(joining)
  }

  private final case class FetchResult(fetchResults: TopicPartitionResults[List[RecordEntry]], commitResults: TopicPartitionResults[Unit])

  sealed trait InitResult
  private final case class InitSuccess(groupResult: JoinGroupResult, offsets: Map[TopicPartition, Long]) extends InitResult
  private final case class InitFailed(error: KafkaResult) extends InitResult

  sealed trait HeartbeatResult
  private case object HeartbeatSuccess extends HeartbeatResult
  private final case class HeartbeatFailed(error: KafkaResult) extends HeartbeatResult

  private object DoHeartbeat

  private final case class State(
    memberId:         String,
    generationId:     Int,
    nextFetchRequest: Map[TopicPartition, Long],
    buffer:           List[TopicPartitionRecordEntry]
  )
}

object KafkaConsumer {
  def props(groupId: String, topicPartitions: Set[TopicPartition], client: KafkaClient, settings: KafkaOperationalSettings) =
    Props(new KafkaConsumer(groupId, topicPartitions, client, settings))
}