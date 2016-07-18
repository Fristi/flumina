package vectos.kafka.types.v0

import scodec._
import scodec.codecs._
import vectos.kafka.types._

final case class OffsetFetchTopicRequest(topic: Option[String], partitions: Vector[Int])
final case class OffsetFetchTopicPartitionResponse(partition: Int, offset: Long, metadata: Option[String], kafkaResult: KafkaResult)
final case class OffsetFetchTopicResponse(topicName: Option[String], partitions: Vector[OffsetFetchTopicPartitionResponse])

object OffsetFetchTopicRequest {
  implicit def codec: Codec[OffsetFetchTopicRequest] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(int32))).as[OffsetFetchTopicRequest]
}

object OffsetFetchTopicPartitionResponse {
  implicit def codec(implicit kafkaResult: Codec[KafkaResult]): Codec[OffsetFetchTopicPartitionResponse] =
    (("partition" | int32) :: ("offset" | int64) :: ("metadata" | kafkaString) :: ("kafkaResult" | kafkaResult)).as[OffsetFetchTopicPartitionResponse]
}

object OffsetFetchTopicResponse {
  implicit def codec(implicit partition: Codec[OffsetFetchTopicPartitionResponse]): Codec[OffsetFetchTopicResponse] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[OffsetFetchTopicResponse]
}