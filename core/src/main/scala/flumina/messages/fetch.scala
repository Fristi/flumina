package flumina.messages

import scodec.Codec
import scodec.codecs._
import flumina._

final case class FetchTopicPartitionRequest(partition: Int, fetchOffset: Long, maxBytes: Int)
final case class FetchTopicRequest(topic: String, partitions: Vector[FetchTopicPartitionRequest])

final case class FetchTopicPartitionResponse(partition: Int, kafkaResult: KafkaResult, highWaterMark: Long, messages: Vector[Message])
final case class FetchTopicResponse(topicName: String, partitions: Vector[FetchTopicPartitionResponse])

object FetchTopicPartitionRequest {
  val codec: Codec[FetchTopicPartitionRequest] =
    (("partition" | int32) :: ("fetch_offset" | int64) :: ("max_bytes" | int32)).as[FetchTopicPartitionRequest]
}

object FetchTopicRequest {
  val codec: Codec[FetchTopicRequest] =
    (("topic" | kafkaRequiredString) :: ("partitions" | kafkaArray(FetchTopicPartitionRequest.codec))).as[FetchTopicRequest]
}

object FetchTopicPartitionResponse {
  val codec: Codec[FetchTopicPartitionResponse] =
    (
      ("partition" | int32) ::
      ("kafkaResult" | KafkaResult.codec) ::
      ("highWaterMark" | int64) ::
      ("messages" | variableSizeBytes(int32, MessageSetCodec.messageSetCodec))
    ).as[FetchTopicPartitionResponse]
}

object FetchTopicResponse {
  val codec: Codec[FetchTopicResponse] =
    (("topic" | kafkaRequiredString) :: ("partitions" | kafkaArray(FetchTopicPartitionResponse.codec))).as[FetchTopicResponse]
}
