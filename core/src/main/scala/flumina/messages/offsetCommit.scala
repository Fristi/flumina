package flumina.messages

import flumina._
import scodec.Codec
import scodec.codecs._

final case class OffsetCommitTopicPartitionRequest(partition: Int, offset: Long, metadata: Option[String])
final case class OffsetCommitTopicRequest(topic: String, partitions: Vector[OffsetCommitTopicPartitionRequest])
final case class OffsetCommitTopicPartitionResponse(partition: Int, kafkaResult: KafkaResult)
final case class OffsetCommitTopicResponse(topicName: String, partitions: Vector[OffsetCommitTopicPartitionResponse])

object OffsetCommitTopicPartitionRequest {
  val codec: Codec[OffsetCommitTopicPartitionRequest] =
    (("partition" | int32) :: ("offset" | int64) :: ("metadata" | kafkaOptionalString)).as[OffsetCommitTopicPartitionRequest]
}

object OffsetCommitTopicRequest {
  val codec: Codec[OffsetCommitTopicRequest] =
    (("topic" | kafkaRequiredString) :: ("offset" | kafkaArray(OffsetCommitTopicPartitionRequest.codec))).as[OffsetCommitTopicRequest]
}

object OffsetCommitTopicPartitionResponse {
  val codec: Codec[OffsetCommitTopicPartitionResponse] =
    (("partition" | int32) :: ("kafkaResult" | KafkaResult.codec)).as[OffsetCommitTopicPartitionResponse]
}

object OffsetCommitTopicResponse {
  val codec: Codec[OffsetCommitTopicResponse] =
    (("topic" | kafkaRequiredString) :: ("offset" | kafkaArray(OffsetCommitTopicPartitionResponse.codec))).as[OffsetCommitTopicResponse]
}
