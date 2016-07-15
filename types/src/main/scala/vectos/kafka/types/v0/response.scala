package vectos.kafka.types.v0

import scodec._
import scodec.bits._
import scodec.codecs._

sealed trait KafkaResponse

object KafkaResponse {

  final case class Produce(topics: Vector[ProduceTopicResponse]) extends KafkaResponse
  final case class Fetch(topics: Vector[FetchTopicResponse]) extends KafkaResponse
  final case class ListOffset(topics: Vector[ListOffsetTopicResponse]) extends KafkaResponse
  final case class Metadata(brokers: Vector[MetadataBrokerResponse], topicMetadata: Vector[MetadataTopicMetadataResponse]) extends KafkaResponse
  final case class OffsetCommit(topics: Vector[OffsetCommitTopicResponse]) extends KafkaResponse
  final case class OffsetFetch(topics: Vector[OffsetFetchTopicResponse]) extends KafkaResponse
  final case class GroupCoordinator(errorCode: KafkaError, coordinatorId: Int, coordinatorHost: Option[String], coordinatorPort: Int) extends KafkaResponse
  final case class JoinGroup(errorCode: KafkaError, generationId: Int, groupProtocol: Option[String], leaderId: Option[String], memberId: Option[String], members: Vector[JoinGroupMemberResponse]) extends KafkaResponse
  final case class Heartbeat(errorCode: KafkaError) extends KafkaResponse
  final case class LeaveGroup(errorCode: KafkaError) extends KafkaResponse
  final case class ListGroups(errorCode: KafkaError, groups: Vector[ListGroupGroupResponse]) extends KafkaResponse
  final case class DescribeGroups(groups: Vector[DescribeGroupsGroupResponse]) extends KafkaResponse

  def produce(implicit topic: Codec[ProduceTopicResponse]): Codec[Produce] =
    ("topics" | kafkaArray(topic)).as[Produce]

  def fetch(implicit topic: Codec[FetchTopicResponse]): Codec[Fetch] =
    ("topics" | kafkaArray(topic)).as[Fetch]

  def listOffset(implicit topic: Codec[ListOffsetTopicResponse]): Codec[ListOffset] =
    ("topics" | kafkaArray(topic)).as[ListOffset]

  def metaData(implicit brokers: Codec[MetadataBrokerResponse], metadata: Codec[MetadataTopicMetadataResponse]): Codec[Metadata] =
    (("brokers" | kafkaArray(brokers)) :: ("metadata" | kafkaArray(metadata))).as[Metadata]

  def offsetCommit(implicit topic: Codec[OffsetCommitTopicResponse]): Codec[OffsetCommit] =
    ("topics" | kafkaArray(topic)).as[OffsetCommit]

  def offsetFetch(implicit topic: Codec[OffsetFetchTopicResponse]): Codec[OffsetFetch] =
    ("topics" | kafkaArray(topic)).as[OffsetFetch]

  def groupCoordinator(implicit kafkaError: Codec[KafkaError]): Codec[GroupCoordinator] =
    (("errorCode" | kafkaError) :: ("coordinatorId" | int32) :: ("coordinatorHost" | kafkaString) :: ("coordinatorPort" | int32)).as[GroupCoordinator]

  def joinGroup(implicit kafkaError: Codec[KafkaError], member: Codec[JoinGroupMemberResponse]): Codec[JoinGroup] =
    (
      ("errorCode" | kafkaError) ::
      ("generationId" | int32) ::
      ("groupProtocol" | kafkaString) ::
      ("leaderId" | kafkaString) ::
      ("memberId" | kafkaString) ::
      ("members" | kafkaArray(member))
    ).as[JoinGroup]

  def heartbeat(implicit kafkaError: Codec[KafkaError]): Codec[Heartbeat] =
    ("errorCode" | kafkaError).as[Heartbeat]

  def leaveGroup(implicit kafkaError: Codec[KafkaError]): Codec[LeaveGroup] =
    ("errorCode" | kafkaError).as[LeaveGroup]

  def listGroups(implicit kafkaError: Codec[KafkaError], group: Codec[ListGroupGroupResponse]): Codec[ListGroups] =
    (("errorCode" | kafkaError) :: ("groups" | kafkaArray(group))).as[ListGroups]

  def describeGroups(implicit group: Codec[DescribeGroupsGroupResponse]): Codec[DescribeGroups] =
    ("groups" | kafkaArray(group)).as[DescribeGroups]
}

final case class ResponseEnvelope(correlationId: Int, response: BitVector)

object ResponseEnvelope {
  implicit val codec: Codec[ResponseEnvelope] = (("correlationId" | int32) :: ("response" | scodec.codecs.bits)).as[ResponseEnvelope]
}