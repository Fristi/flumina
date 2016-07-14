package vectos.kafka.types

import scodec._
import scodec.bits.{crc, _}
import scodec.codecs._

sealed trait KafkaResponse

object KafkaResponse {

  final case class Produce(topics: Vector[ProduceTopicResponse]) extends KafkaResponse
  final case class Fetch(throttleTime: Int, topics: Vector[FetchTopicResponse]) extends KafkaResponse
  final case class ListOffset(topics: Vector[ListOffsetTopicResponse]) extends KafkaResponse
  final case class Metadata(brokers: Vector[MetadataBrokerResponse], topicMetadata: Vector[MetadataTopicMetadataResponse]) extends KafkaResponse
  final case class GroupCoordinator(errorCode: KafkaError, coordinatorId: Int, coordinatorHost: String, coordinatorPort: Int) extends KafkaResponse

  def produce(implicit topic: Codec[ProduceTopicResponse]): Codec[Produce] =
    ("topics" | kafkaArray(topic)).as[Produce]

  def fetch(implicit topic: Codec[FetchTopicResponse]): Codec[Fetch] =
    (("throttleTime" | int32) :: ("topics" | kafkaArray(topic))).as[Fetch]

  def listOffset(implicit topic: Codec[ListOffsetTopicResponse]): Codec[ListOffset] =
    ("topics" | kafkaArray(topic)).as[ListOffset]

  def metaData(implicit brokers: Codec[MetadataBrokerResponse], metadata: Codec[MetadataTopicMetadataResponse]): Codec[Metadata] =
    (("brokers" | kafkaArray(brokers)) :: ("metadata" | kafkaArray(metadata))).as[Metadata]

  def groupCoordinator(implicit kafkaError: Codec[KafkaError]): Codec[GroupCoordinator] =
    (("errorCode" | kafkaError) :: ("coordinatorId" | int32) :: ("coordinatorHost" | kafkaString) :: ("coordinatorPort" | int32)).as[GroupCoordinator]
}

case class ResponseEnvelope(correlationId: Int, response: BitVector)

object ResponseEnvelope {
  implicit val codec = (("correlationId" | int32) :: ("response" | scodec.codecs.bits)).as[ResponseEnvelope]
}