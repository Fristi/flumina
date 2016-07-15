package vectos.kafka.types.v0

import scodec._
import scodec.codecs._

final case class OffsetFetchTopicRequest(topic: Option[String], partitions: Vector[Int])
final case class OffsetFetchTopicPartitionResponse(partition: Int, offset: Long, metadata: Option[String], errorCode: KafkaError)
final case class OffsetFetchTopicResponse(topic: Option[String], partitions: Vector[OffsetFetchTopicPartitionResponse])

object OffsetFetchTopicRequest {
  implicit def codec: Codec[OffsetFetchTopicRequest] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(int32))).as[OffsetFetchTopicRequest]
}

object OffsetFetchTopicPartitionResponse {
  implicit def codec(implicit kafkaError: Codec[KafkaError]): Codec[OffsetFetchTopicPartitionResponse] =
    (("partition" | int32) :: ("offset" | int64) :: ("metadata" | kafkaString) :: ("errorCode" | kafkaError)).as[OffsetFetchTopicPartitionResponse]
}

object OffsetFetchTopicResponse {
  implicit def codec(implicit partition: Codec[OffsetFetchTopicPartitionResponse]): Codec[OffsetFetchTopicResponse] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[OffsetFetchTopicResponse]
}