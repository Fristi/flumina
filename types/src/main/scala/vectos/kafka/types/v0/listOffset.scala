package vectos.kafka.types.v0

import scodec._
import scodec.codecs._

final case class ListOffsetTopicPartitionRequest(partition: Int, time: Long, maxNumberOfOffsets: Int)
final case class ListOffsetTopicRequest(topic: Option[String], partitions: Vector[ListOffsetTopicPartitionRequest])
final case class ListOffsetTopicPartitionResponse(partition: Int, errorCode: KafkaError, offsets: Vector[Long])
final case class ListOffsetTopicResponse(topic: Option[String], partitions: Vector[ListOffsetTopicPartitionResponse])

object ListOffsetTopicPartitionRequest {
  implicit def codec: Codec[ListOffsetTopicPartitionRequest] =
    (("partition" | int32) :: ("time" | int64) :: ("maxNumberOfOffsets" | int32)).as[ListOffsetTopicPartitionRequest]
}

object ListOffsetTopicRequest {
  implicit def codec(implicit partition: Codec[ListOffsetTopicPartitionRequest]): Codec[ListOffsetTopicRequest] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[ListOffsetTopicRequest]
}

object ListOffsetTopicPartitionResponse {
  implicit def codec(implicit kafkaError: Codec[KafkaError]): Codec[ListOffsetTopicPartitionResponse] =
    (("partition" | int32) :: ("errorCode" | kafkaError) :: ("offsets" | kafkaArray(int64))).as[ListOffsetTopicPartitionResponse]
}

object ListOffsetTopicResponse {
  implicit def codec(implicit partition: Codec[ListOffsetTopicPartitionResponse]): Codec[ListOffsetTopicResponse] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[ListOffsetTopicResponse]
}