package vectos.kafka.types.v0

import scodec.Codec
import scodec.codecs._
import vectos.kafka.types._

final case class FetchTopicPartitionRequest(partition: Int, fetchOffset: Long, maxBytes: Int)
final case class FetchTopicRequest(topic: Option[String], partitions: Vector[FetchTopicPartitionRequest])

final case class FetchTopicPartitionResponse(partition: Int, kafkaResult: KafkaResult, highWaterMark: Long, messages: Vector[MessageSetEntry])
final case class FetchTopicResponse(topicName: Option[String], partitions: Vector[FetchTopicPartitionResponse])

object FetchTopicPartitionRequest {
  implicit def codec: Codec[FetchTopicPartitionRequest] =
    (("partition" | int32) :: ("fetch_offset" | int64) :: ("max_bytes" | int32)).as[FetchTopicPartitionRequest]
}

object FetchTopicRequest {
  implicit def codec(implicit partition: Codec[FetchTopicPartitionRequest]): Codec[FetchTopicRequest] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[FetchTopicRequest]
}

object FetchTopicPartitionResponse {
  implicit def codec(implicit messageSetEntry: Codec[MessageSetEntry], kafkaResult: Codec[KafkaResult]): Codec[FetchTopicPartitionResponse] =
    (
      ("partition" | int32) ::
      ("kafkaResult" | kafkaResult) ::
      ("highWaterMark" | int64) ::
      ("messages" | variableSizeBytes(int32, partialVector(messageSetEntry)))
    ).as[FetchTopicPartitionResponse]
}

object FetchTopicResponse {
  implicit def codec(implicit partition: Codec[FetchTopicPartitionResponse]): Codec[FetchTopicResponse] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[FetchTopicResponse]
}
