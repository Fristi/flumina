package flumina.core.v090

import scodec._
import scodec.codecs._
import flumina.core._

final case class OffsetFetchTopicRequest(topic: String, partitions: Vector[Int])
final case class OffsetFetchTopicPartitionResponse(partition: Int, offset: Long, metadata: Option[String], kafkaResult: KafkaResult)
final case class OffsetFetchTopicResponse(topicName: String, partitions: Vector[OffsetFetchTopicPartitionResponse])

object OffsetFetchTopicRequest {
  implicit def codec: Codec[OffsetFetchTopicRequest] =
    (("topic" | kafkaRequiredString) :: ("partitions" | kafkaArray(int32))).as[OffsetFetchTopicRequest]
}

object OffsetFetchTopicPartitionResponse {
  implicit def codec(implicit kafkaResult: Codec[KafkaResult]): Codec[OffsetFetchTopicPartitionResponse] =
    (("partition" | int32) :: ("offset" | int64) :: ("metadata" | kafkaOptionalString) :: ("kafkaResult" | kafkaResult)).as[OffsetFetchTopicPartitionResponse]
}

object OffsetFetchTopicResponse {
  implicit def codec(implicit partition: Codec[OffsetFetchTopicPartitionResponse]): Codec[OffsetFetchTopicResponse] =
    (("topic" | kafkaRequiredString) :: ("partitions" | kafkaArray(partition))).as[OffsetFetchTopicResponse]
}