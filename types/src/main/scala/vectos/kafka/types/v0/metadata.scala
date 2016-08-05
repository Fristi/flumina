package vectos.kafka.types.v0

import scodec.Codec
import scodec.codecs._
import vectos.kafka.types._

final case class MetadataBrokerResponse(nodeId: Int, host: Option[String], port: Int)

final case class MetadataTopicPartitionMetadataResponse(
  kafkaResult: KafkaResult,
  id:          Int,
  leader:      Int,
  replicas:    Vector[Int],
  isr:         Vector[Int]
)

final case class MetadataTopicMetadataResponse(
  kafkaResult: KafkaResult,
  topicName:   Option[String],
  partitions:  Vector[MetadataTopicPartitionMetadataResponse]
)

object MetadataBrokerResponse {
  implicit val codec: Codec[MetadataBrokerResponse] =
    (("nodeId" | int32) :: ("host" | kafkaString) :: ("port" | int32)).as[MetadataBrokerResponse]
}

object MetadataTopicPartitionMetadataResponse {
  implicit def codec(implicit kafkaResult: Codec[KafkaResult]): Codec[MetadataTopicPartitionMetadataResponse] = (
    ("kafkaResult" | kafkaResult) ::
    ("id" | int32) ::
    ("leader" | int32) ::
    ("replicaes" | kafkaArray(int32)) ::
    ("isr" | kafkaArray(int32))
  ).as[MetadataTopicPartitionMetadataResponse]
}

object MetadataTopicMetadataResponse {
  implicit def codec(implicit kafkaResult: Codec[KafkaResult], metadata: Codec[MetadataTopicPartitionMetadataResponse]): Codec[MetadataTopicMetadataResponse] =
    (("kafkaResult" | kafkaResult) :: ("name" | kafkaString) :: ("partitions" | kafkaArray(metadata))).as[MetadataTopicMetadataResponse]
}