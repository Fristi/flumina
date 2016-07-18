package vectos.kafka.types.v0

import scodec._
import scodec.codecs._
import vectos.kafka.types._

final case class ListOffsetTopicPartitionRequest(partition: Int, time: Long, maxNumberOfOffsets: Int)
final case class ListOffsetTopicRequest(topic: Option[String], partitions: Vector[ListOffsetTopicPartitionRequest])
final case class ListOffsetTopicPartitionResponse(partition: Int, kafkaResult: KafkaResult, offsets: Vector[Long])
final case class ListOffsetTopicResponse(topicName: Option[String], partitions: Vector[ListOffsetTopicPartitionResponse])

object ListOffsetTopicPartitionRequest {
  implicit def codec: Codec[ListOffsetTopicPartitionRequest] =
    (("partition" | int32) :: ("time" | int64) :: ("maxNumberOfOffsets" | int32)).as[ListOffsetTopicPartitionRequest]
}

object ListOffsetTopicRequest {
  implicit def codec(implicit partition: Codec[ListOffsetTopicPartitionRequest]): Codec[ListOffsetTopicRequest] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[ListOffsetTopicRequest]
}

object ListOffsetTopicPartitionResponse {
  implicit def codec(implicit kafkaResult: Codec[KafkaResult]): Codec[ListOffsetTopicPartitionResponse] =
    (("partition" | int32) :: ("kafkaResult" | kafkaResult) :: ("offsets" | kafkaArray(int64))).as[ListOffsetTopicPartitionResponse]
}

object ListOffsetTopicResponse {
  implicit def codec(implicit partition: Codec[ListOffsetTopicPartitionResponse]): Codec[ListOffsetTopicResponse] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[ListOffsetTopicResponse]
}