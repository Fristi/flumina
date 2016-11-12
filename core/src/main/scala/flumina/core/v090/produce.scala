package flumina.core.v090

import scodec.Codec
import scodec.codecs._
import flumina.core._

final case class ProduceTopicPartitionRequest(partition: Int, messageSets: Vector[Message])
final case class ProduceTopicRequest(topicName: String, partitions: Vector[ProduceTopicPartitionRequest])

final case class ProduceTopicPartitionResponse(partition: Int, kafkaResult: KafkaResult, offset: Long)
final case class ProduceTopicResponse(topicName: String, partitions: Vector[ProduceTopicPartitionResponse])

object ProduceTopicPartitionRequest {
  implicit def codec: Codec[ProduceTopicPartitionRequest] =
    (("partition" | int32) :: ("message" | variableSizeBytes(int32, MessageSetCodec.messageSetCodec))).as[ProduceTopicPartitionRequest]
}

object ProduceTopicRequest {
  implicit def codec(implicit partition: Codec[ProduceTopicPartitionRequest]): Codec[ProduceTopicRequest] =
    (("name" | kafkaRequiredString) :: ("partitions" | kafkaArray(partition))).as[ProduceTopicRequest]
}

object ProduceTopicPartitionResponse {
  implicit def codec(implicit kafkaResult: Codec[KafkaResult]): Codec[ProduceTopicPartitionResponse] =
    (("partition" | int32) :: ("kafkaResult" | kafkaResult) :: ("offset" | int64)).as[ProduceTopicPartitionResponse]
}

object ProduceTopicResponse {
  implicit def codec(implicit partition: Codec[ProduceTopicPartitionResponse]): Codec[ProduceTopicResponse] =
    (("name" | kafkaRequiredString) :: ("partitions" | kafkaArray(partition))).as[ProduceTopicResponse]
}