package flumina.messages

import scodec.Codec
import scodec.codecs._
import flumina._

final case class ProduceTopicPartitionRequest(partition: Int, messageSets: Vector[Message])
final case class ProduceTopicRequest(topicName: String, partitions: Vector[ProduceTopicPartitionRequest])

final case class ProduceTopicPartitionResponse(partition: Int, kafkaResult: KafkaResult, offset: Long)
final case class ProduceTopicResponse(topicName: String, partitions: Vector[ProduceTopicPartitionResponse])

object ProduceTopicPartitionRequest {
  val codec: Codec[ProduceTopicPartitionRequest] =
    (("partition" | int32) :: ("message" | variableSizeBytes(int32, MessageSetCodec.messageSetCodec))).as[ProduceTopicPartitionRequest]
}

object ProduceTopicRequest {
  val codec: Codec[ProduceTopicRequest] =
    (("name" | kafkaRequiredString) :: ("partitions" | kafkaArray(ProduceTopicPartitionRequest.codec))).as[ProduceTopicRequest]
}

object ProduceTopicPartitionResponse {
  val codec: Codec[ProduceTopicPartitionResponse] =
    (("partition" | int32) :: ("kafkaResult" | KafkaResult.codec) :: ("offset" | int64)).as[ProduceTopicPartitionResponse]
}

object ProduceTopicResponse {
  val codec: Codec[ProduceTopicResponse] =
    (("name" | kafkaRequiredString) :: ("partitions" | kafkaArray(ProduceTopicPartitionResponse.codec))).as[ProduceTopicResponse]
}