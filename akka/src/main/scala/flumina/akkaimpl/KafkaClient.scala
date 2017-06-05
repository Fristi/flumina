package flumina.akkaimpl

import akka.actor.ActorSystem
import akka.util.Timeout
import cats.free.Free
import cats.implicits._
import flumina.core.ir._
import flumina.core.{KafkaA, KafkaResult}
import pureconfig.loadConfig

import scala.concurrent.{ExecutionContext, Future}

final class KafkaClient private (settings: KafkaSettings)(implicit S: ActorSystem, EC: ExecutionContext) {

  private implicit val requestTimeout: Timeout = Timeout(settings.requestTimeout)

  private val int = new AkkaInterpreter(settings)

  def run[A](kafka: Free[KafkaA, A]): Future[A] = kafka.foldMap(int)

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

  def fetch(topicPartitionOffsets: Set[TopicPartitionValue[Long]]): Future[TopicPartitionValues[OffsetValue[Record]]] =
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

  def apply()(implicit S: ActorSystem, EC: ExecutionContext): KafkaClient =
    new KafkaClient(loadConfig[KafkaSettings](S.settings.config.getConfig("flumina")).fold(errs => sys.error(s"Error loading config: $errs"), identity))
}