package flumina.messages

import scodec.Codec
import scodec.codecs._
import flumina._

final case class MetadataBrokerResponse(nodeId: Int, host: String, port: Int, rack: Option[String])

final case class MetadataTopicPartitionMetadataResponse(
  kafkaResult: KafkaResult,
  id:          Int,
  leader:      Int,
  replicas:    Vector[Int],
  isr:         Vector[Int]
)

final case class MetadataTopicMetadataResponse(
  kafkaResult: KafkaResult,
  topicName:   String,
  isInternal:  Boolean,
  partitions:  Vector[MetadataTopicPartitionMetadataResponse]
)

object MetadataBrokerResponse {
  val codec: Codec[MetadataBrokerResponse] =
    (
      ("nodeId" | int32) ::
      ("host" | kafkaRequiredString) ::
      ("port" | int32) ::
      ("rack" | kafkaOptionalString)
    ).as[MetadataBrokerResponse]
}

object MetadataTopicPartitionMetadataResponse {
  val codec: Codec[MetadataTopicPartitionMetadataResponse] = (
    ("kafkaResult" | KafkaResult.codec) ::
    ("id" | int32) ::
    ("leader" | int32) ::
    ("replicaes" | kafkaArray(int32)) ::
    ("isr" | kafkaArray(int32))
  ).as[MetadataTopicPartitionMetadataResponse]
}

object MetadataTopicMetadataResponse {
  val codec: Codec[MetadataTopicMetadataResponse] =
    (
      ("kafkaResult" | KafkaResult.codec) ::
      ("name" | kafkaRequiredString) ::
      ("isInternal" | kafkaBool) ::
      ("partitions" | kafkaArray(MetadataTopicPartitionMetadataResponse.codec))
    ).as[MetadataTopicMetadataResponse]
}