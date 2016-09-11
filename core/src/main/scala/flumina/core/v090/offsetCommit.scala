package flumina.core.v090

import scodec.Codec
import scodec.codecs._
import flumina.core._

final case class OffsetCommitTopicPartitionRequest(partition: Int, offset: Long, metadata: Option[String])
final case class OffsetCommitTopicRequest(topic: String, partitions: Vector[OffsetCommitTopicPartitionRequest])
final case class OffsetCommitTopicPartitionResponse(partition: Int, kafkaResult: KafkaResult)
final case class OffsetCommitTopicResponse(topicName: String, partitions: Vector[OffsetCommitTopicPartitionResponse])

object OffsetCommitTopicPartitionRequest {
  implicit def codec: Codec[OffsetCommitTopicPartitionRequest] =
    (("partition" | int32) :: ("offset" | int64) :: ("metadata" | kafkaOptionalString)).as[OffsetCommitTopicPartitionRequest]
}

object OffsetCommitTopicRequest {
  implicit def codec(implicit partition: Codec[OffsetCommitTopicPartitionRequest]): Codec[OffsetCommitTopicRequest] =
    (("topic" | kafkaRequiredString) :: ("offset" | kafkaArray(partition))).as[OffsetCommitTopicRequest]
}

object OffsetCommitTopicPartitionResponse {
  implicit def codec(implicit kafkaResult: Codec[KafkaResult]): Codec[OffsetCommitTopicPartitionResponse] =
    (("partition" | int32) :: ("kafkaResult" | kafkaResult)).as[OffsetCommitTopicPartitionResponse]
}

object OffsetCommitTopicResponse {
  implicit def codec(implicit partition: Codec[OffsetCommitTopicPartitionResponse]): Codec[OffsetCommitTopicResponse] =
    (("topic" | kafkaRequiredString) :: ("offset" | kafkaArray(partition))).as[OffsetCommitTopicResponse]
}
