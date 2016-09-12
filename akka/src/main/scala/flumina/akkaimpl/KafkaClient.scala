package flumina.akkaimpl

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import cats.data.Xor
import com.typesafe.config.Config
import flumina.core.ir._
import flumina.core.{KafkaAlg, KafkaResult}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

final class KafkaClient private (settings: KafkaSettings, actorSystem: ActorSystem) extends KafkaAlg[Future] {

  private implicit val timeout: Timeout = Timeout(settings.requestTimeout)
  private implicit val executionContext: ExecutionContext = actorSystem.dispatcher

  private val coordinator = actorSystem.actorOf(KafkaCoordinator.props(settings))

  def producer(grouped: Int, parallelism: Int) =
    Flow[(TopicPartition, Record)]
      .grouped(grouped)
      .mapAsync(parallelism)(x => produce(x.toList))
      .to(Sink.ignore)

  def consume(groupId: String, topicPartitions: Set[TopicPartition]): Source[TopicPartitionRecordEntry, ActorRef] =
    Source.actorPublisher[TopicPartitionRecordEntry](KafkaConsumer.props(groupId, topicPartitions, this, settings.operationalSettings))

  //TODO: should we check the invariant topicPartition.partition >= 0?
  def produce(values: List[(TopicPartition, Record)]) =
    (coordinator ? KafkaCoordinator.Produce(values)).mapTo[TopicPartitionResults[Long]]

  def offsetFetch(groupId: String, values: Set[TopicPartition]) =
    (coordinator ? KafkaCoordinator.OffsetsFetch(groupId, values)).mapTo[TopicPartitionResults[OffsetMetadata]]

  def offsetCommit(groupId: String, generationId: Int, memberId: String, offsets: Map[TopicPartition, OffsetMetadata]) =
    (coordinator ? KafkaCoordinator.OffsetsCommit(groupId, generationId, memberId, offsets)).mapTo[TopicPartitionResults[Unit]]

  def fetch(values: Map[TopicPartition, Long]) =
    (coordinator ? KafkaCoordinator.Fetch(values)).mapTo[TopicPartitionResults[List[RecordEntry]]]

  def joinGroup(groupId: String, memberId: Option[String], protocol: String, protocols: Seq[GroupProtocol]) =
    (coordinator ? KafkaCoordinator.JoinGroup(groupId, memberId, protocol, protocols)).mapTo[KafkaResult Xor JoinGroupResult]

  def syncGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment]) =
    (coordinator ? KafkaCoordinator.SynchronizeGroup(groupId, generationId, memberId, assignments)).mapTo[KafkaResult Xor MemberAssignment]

  def heartbeat(groupId: String, generationId: Int, memberId: String) =
    (coordinator ? KafkaCoordinator.Heartbeat(groupId, generationId, memberId)).mapTo[KafkaResult Xor Unit]

  def metadata(topics: Set[String]): Future[Metadata] =
    (coordinator ? KafkaCoordinator.Metadata(topics)).mapTo[Metadata]

  def groupCoordinator(groupId: String): Future[Xor[KafkaResult, Broker]] =
    (coordinator ? KafkaCoordinator.GroupCoordinator(groupId)).mapTo[KafkaResult Xor Broker]

  def leaveGroup(groupId: String, memberId: String) =
    (coordinator ? KafkaCoordinator.LeaveGroup(groupId, memberId)).mapTo[KafkaResult Xor Unit]

  def listGroups =
    (coordinator ? KafkaCoordinator.ListGroups).mapTo[KafkaResult Xor List[GroupInfo]]

  def pure[A](x: A) = Future.successful(x)

  def flatMap[A, B](fa: Future[A])(f: (A) => Future[B]) = fa.flatMap(f)

  def tailRecM[A, B](a: A)(f: (A) => Future[Either[A, B]]): Future[B] = flatMap(f(a)) {
    case Left(ohh)  => tailRecM(ohh)(f)
    case Right(ohh) => pure(ohh)
  }
}

object KafkaClient {

  def apply()(implicit system: ActorSystem) =
    new KafkaClient(new KafkaConfig(system).settings, system)

  private final class KafkaConfig(system: ActorSystem) {

    val config: Config = system.settings.config

    private def getDuration(key: String): FiniteDuration = FiniteDuration(config.getDuration(key).toNanos, TimeUnit.NANOSECONDS)

    val bootstrapBrokers = config.getStringList("flumina.bootstrap-brokers").asScala
    val connectionsPerBroker = config.getInt("flumina.connections-per-broker")
    val retryBackoff = getDuration("flumina.retry-backoff")
    val fetchMaxWaitTime = getDuration("flumina.fetch-max-wait-time")
    val produceTimeout = getDuration("flumina.produce-timeout")
    val groupSessionTimeout = getDuration("flumina.group-session-timeout")
    val requestTimeout = getDuration("flumina.request-timeout")
    val retryMaxCount = config.getInt("flumina.retry-max-count")
    val fetchMaxBytes = config.getInt("flumina.fetch-max-bytes")
    val heartbeatFrequency = config.getInt("flumina.heartbeat-frequency")

    val brokers = bootstrapBrokers.map { x =>
      val parts = x.split(":").toList
      (for {
        host <- parts.headOption
        portRaw <- parts.lift(1)
        port <- Try(portRaw.toInt).toOption
      } yield KafkaBroker.Node(host, port)) getOrElse (throw new Exception(s"Unable to convert: $x to a broker (format = host:port)"))
    }

    val settings = KafkaSettings(
      brokers,
      connectionsPerBroker,
      KafkaOperationalSettings(
        retryBackoff,
        retryMaxCount,
        fetchMaxWaitTime,
        fetchMaxBytes,
        produceTimeout,
        groupSessionTimeout,
        heartbeatFrequency,
        ConsumeAssignmentStrategy.allToLeader
      ),
      requestTimeout
    )
  }

}