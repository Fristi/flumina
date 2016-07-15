package vectos.kafka.types.v0

import scodec.Codec
import scodec.codecs._

final case class OffsetCommitTopicPartitionRequest(partition: Int, offset: Long, metadata: Option[String])
final case class OffsetCommitTopicRequest(topic: Option[String], partitions: Vector[OffsetCommitTopicPartitionRequest])
final case class OffsetCommitTopicPartitionResponse(partition: Int, errorCode: KafkaError)
final case class OffsetCommitTopicResponse(topic: Option[String], partitions: Vector[OffsetCommitTopicPartitionResponse])

object OffsetCommitTopicPartitionRequest {
  implicit def codec: Codec[OffsetCommitTopicPartitionRequest] =
    (("partition" | int32) :: ("offset" | int64) :: ("metadata" | kafkaString)).as[OffsetCommitTopicPartitionRequest]
}

object OffsetCommitTopicRequest {
  implicit def codec(implicit partition: Codec[OffsetCommitTopicPartitionRequest]): Codec[OffsetCommitTopicRequest] =
    (("topic" | kafkaString) :: ("offset" | kafkaArray(partition))).as[OffsetCommitTopicRequest]
}

object OffsetCommitTopicPartitionResponse {
  implicit def codec(implicit kafkaError: Codec[KafkaError]): Codec[OffsetCommitTopicPartitionResponse] =
    (("partition" | int32) :: ("errorCode" | kafkaError)).as[OffsetCommitTopicPartitionResponse]
}

object OffsetCommitTopicResponse {
  implicit def codec(implicit partition: Codec[OffsetCommitTopicPartitionResponse]): Codec[OffsetCommitTopicResponse] =
    (("topic" | kafkaString) :: ("offset" | kafkaArray(partition))).as[OffsetCommitTopicResponse]
}
