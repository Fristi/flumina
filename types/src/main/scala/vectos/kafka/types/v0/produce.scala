package vectos.kafka.types.v0

import scodec.Codec
import scodec.codecs._

/**
  * Created by mark on 07/07/16.
  */
trait ProduceTypes {

  final case class ProduceTopicPartitionRequest(partition: Int, messageSets: Vector[MessageSetEntry])
  final case class ProduceTopicRequest(topicName: String, partitions: Vector[ProduceTopicPartitionRequest])

  final case class ProduceTopicPartitionResponse(partition: Int, errorCode: KafkaError, offset: Long)
  final case class ProduceTopicResponse(topicName: String, partitions: Vector[ProduceTopicPartitionResponse])

  object ProduceTopicPartitionRequest {
    implicit def codec(implicit messageSet: Codec[MessageSetEntry]): Codec[ProduceTopicPartitionRequest] =
      (("partition" | int32) :: ("message" | variableSizeBytes(int32, vector(messageSet)))).as[ProduceTopicPartitionRequest]
  }

  object ProduceTopicRequest {
    implicit def codec(implicit partition: Codec[ProduceTopicPartitionRequest]): Codec[ProduceTopicRequest] =
      (("name" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[ProduceTopicRequest]
  }

  object ProduceTopicPartitionResponse {
    implicit def codec(implicit kafkaError: Codec[KafkaError]): Codec[ProduceTopicPartitionResponse] =
      (("partition" | int32) :: ("errorCode" | kafkaError) :: ("offset" | int64)).as[ProduceTopicPartitionResponse]
  }

  object ProduceTopicResponse {
    implicit def codec(implicit partition: Codec[ProduceTopicPartitionResponse]): Codec[ProduceTopicResponse] =
      (("name" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[ProduceTopicResponse]
  }
}
