package vectos.kafka.types.v0.messages

import scodec._
import scodec.codecs._

trait ListOffsetTypes {

  final case class ListOffsetTopicPartitionRequest(partition: Int, time: Long, maxNumberOfOffsets: Int)

  object ListOffsetTopicPartitionRequest {
    implicit def codec: Codec[ListOffsetTopicPartitionRequest] =
      (("partition" | int32) :: ("time" | int64) :: ("maxNumberOfOffsets" | int32)).as[ListOffsetTopicPartitionRequest]
  }

  final case class ListOffsetTopicRequest(topic: Option[String], partitions: Vector[ListOffsetTopicPartitionRequest])

  object ListOffsetTopicRequest {
    implicit def codec(implicit partition: Codec[ListOffsetTopicPartitionRequest]): Codec[ListOffsetTopicRequest] =
      (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[ListOffsetTopicRequest]
  }

  final case class ListOffsetTopicPartitionResponse(partition: Int, errorCode: KafkaError, offsets: Vector[Long])

  object ListOffsetTopicPartitionResponse {
    implicit def codec(implicit kafkaError: Codec[KafkaError]): Codec[ListOffsetTopicPartitionResponse] =
      (("partition" | int32) :: ("errorCode" | kafkaError) :: ("offsets" | kafkaArray(int64))).as[ListOffsetTopicPartitionResponse]
  }

  final case class ListOffsetTopicResponse(topic: Option[String], partitions: Vector[ListOffsetTopicPartitionResponse])

  object ListOffsetTopicResponse {
    implicit def codec(implicit partition: Codec[ListOffsetTopicPartitionResponse]): Codec[ListOffsetTopicResponse] =
      (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[ListOffsetTopicResponse]
  }
}