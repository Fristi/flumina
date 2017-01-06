package flumina.akkaimpl

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.util.Timeout
import cats.free.Free
import cats.implicits._
import com.typesafe.config.Config
import flumina.core.ir._
import flumina.core.{KafkaA, KafkaResult}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

final class KafkaClient(settings: KafkaSettings)(implicit S: ActorSystem, T: Timeout, EC: ExecutionContext) {

  private val int = new AkkaInterpreter(settings)

  def run[A](kafka: Free[KafkaA, A]) = kafka.foldMap(int)

  def createTopics(topics: Set[TopicDescriptor]): Future[List[TopicResult]] =
    int(KafkaA.CreateTopics(topics))

  def deleteTopics(topics: Set[String]): Future[List[TopicResult]] =
    int(KafkaA.DeleteTopics(topics))

  def metadata(topics: Set[String]): Future[Metadata] =
    int(KafkaA.GetMetadata(topics))

  def groupCoordinator(groupId: String): Future[KafkaResult Either Broker] =
    int(KafkaA.GroupCoordinator(groupId))

  def produceOne(values: TopicPartitionValue[Record]): Future[TopicPartitionValues[Long]] =
    int(KafkaA.ProduceOne(values))

  def produceN(compression: Compression, values: Seq[TopicPartitionValue[Record]]): Future[TopicPartitionValues[Long]] =
    int(KafkaA.ProduceN(compression, values))

  def fetch(topicPartitionOffsets: Set[TopicPartitionValue[Long]]): Future[TopicPartitionValues[List[RecordEntry]]] =
    int(KafkaA.Fetch(topicPartitionOffsets))

  def offsetFetch(groupId: String, topicPartitions: Set[TopicPartition]): Future[TopicPartitionValues[OffsetMetadata]] =
    int(KafkaA.OffsetsFetch(groupId, topicPartitions))

  def offsetCommit(groupId: String, generationId: Int, memberId: String, offsets: Map[TopicPartition, OffsetMetadata]): Future[TopicPartitionValues[Unit]] =
    int(KafkaA.OffsetsCommit(groupId, generationId, memberId, offsets))

  def joinGroup(groupId: String, memberId: Option[String], protocol: String, protocols: Seq[GroupProtocol]): Future[KafkaResult Either JoinGroupResult] =
    int(KafkaA.JoinGroup(groupId, memberId, protocol, protocols))

  def leaveGroup(groupId: String, memberId: String): Future[Either[KafkaResult, Unit]] =
    int(KafkaA.LeaveGroup(groupId, memberId))

  def heartbeat(groupId: String, generationId: Int, memberId: String): Future[Either[KafkaResult, Unit]] =
    int(KafkaA.Heartbeat(groupId, generationId, memberId))

  def syncGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment]): Future[KafkaResult Either MemberAssignment] =
    int(KafkaA.SynchronizeGroup(groupId, generationId, memberId, assignments))

  def listGroups: Future[KafkaResult Either List[GroupInfo]] =
    int(KafkaA.ListGroups)

  def describeGroups(groupIds: Set[String]): Future[Seq[Group]] =
    int(KafkaA.DescribeGroups(groupIds))

  def apiVersions: Future[KafkaResult Either Seq[ApiVersion]] =
    int(KafkaA.ApiVersions)

}

object KafkaClient {

  def apply()(implicit S: ActorSystem, T: Timeout, EC: ExecutionContext) = new KafkaClient(new KafkaConfig(S).settings)

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
        groupSessionTimeout
      ),
      requestTimeout
    )
  }

}