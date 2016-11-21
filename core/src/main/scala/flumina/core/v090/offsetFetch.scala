package flumina.core.v090

import scodec._
import scodec.codecs._
import flumina.core.{KafkaResult, _}

final case class OffsetFetchTopicRequest(topic: String, partitions: Vector[Int])
final case class OffsetFetchTopicPartitionResponse(partition: Int, offset: Long, metadata: Option[String], kafkaResult: KafkaResult)
final case class OffsetFetchTopicResponse(topicName: String, partitions: Vector[OffsetFetchTopicPartitionResponse])

object OffsetFetchTopicRequest {
  val codec: Codec[OffsetFetchTopicRequest] =
    (("topic" | kafkaRequiredString) :: ("partitions" | kafkaArray(int32))).as[OffsetFetchTopicRequest]
}

object OffsetFetchTopicPartitionResponse {
  val codec: Codec[OffsetFetchTopicPartitionResponse] =
    (("partition" | int32) :: ("offset" | int64) :: ("metadata" | kafkaOptionalString) :: ("kafkaResult" | KafkaResult.codec)).as[OffsetFetchTopicPartitionResponse]
}

object OffsetFetchTopicResponse {
  val codec: Codec[OffsetFetchTopicResponse] =
    (("topic" | kafkaRequiredString) :: ("partitions" | kafkaArray(OffsetFetchTopicPartitionResponse.codec))).as[OffsetFetchTopicResponse]
}