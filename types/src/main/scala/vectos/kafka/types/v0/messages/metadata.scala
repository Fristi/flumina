package vectos.kafka.types.v0.messages

import scodec.Codec
import scodec.codecs._

final case class MetadataBrokerResponse(nodeId: Int, host: Option[String], port: Int)

final case class MetadataTopicPartitionMetadataResponse(
  errorCode: KafkaError,
  id:        Int,
  leader:    Int,
  replicas:  Vector[Int],
  isr:       Vector[Int]
)

final case class MetadataTopicMetadataResponse(
  errorCode:         KafkaError,
  name:              Option[String],
  partitionMetaData: Vector[MetadataTopicPartitionMetadataResponse]
)

object MetadataBrokerResponse {
  implicit val codec: Codec[MetadataBrokerResponse] =
    (("nodeId" | int32) :: ("host" | kafkaString) :: ("port" | int32)).as[MetadataBrokerResponse]
}

object MetadataTopicPartitionMetadataResponse {
  implicit def codec(implicit kafkaError: Codec[KafkaError]): Codec[MetadataTopicPartitionMetadataResponse] = (
    ("errorCode" | kafkaError) ::
    ("id" | int32) ::
    ("leader" | int32) ::
    ("replicaes" | kafkaArray(int32)) ::
    ("isr" | kafkaArray(int32))
  ).as[MetadataTopicPartitionMetadataResponse]
}

object MetadataTopicMetadataResponse {
  implicit def codec(implicit kafkaError: Codec[KafkaError], metadata: Codec[MetadataTopicPartitionMetadataResponse]): Codec[MetadataTopicMetadataResponse] =
    (("errorCode" | kafkaError) :: ("name" | kafkaString) :: ("partitions" | kafkaArray(metadata))).as[MetadataTopicMetadataResponse]
}