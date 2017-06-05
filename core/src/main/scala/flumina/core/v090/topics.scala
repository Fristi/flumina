package flumina.core.v090

import scodec.codecs._
import flumina.core._
import scodec.Codec
final case class ReplicaAssignment(partitionId: Int, replicas: Vector[Int])

object ReplicaAssignment {
  val codec: Codec[ReplicaAssignment] = (("partitionId" | int32) :: ("replicas" | kafkaArray(int32))).as[ReplicaAssignment]
}

final case class CreateTopicRequest(
  topic:             String,
  nrPartitions:      Option[Int],
  replicationFactor: Option[Int],
  replicaAssignment: Vector[ReplicaAssignment],
  config:            Map[String, String]
)

object CreateTopicRequest {

  val codec: Codec[CreateTopicRequest] =
    (
      ("partitionId" | kafkaRequiredString) ::
      ("nrPartitions" | kafkaOptionalInt32) ::
      ("replicationFactor" | kafkaOptionalInt16) ::
      ("replicaAssignment" | kafkaArray(ReplicaAssignment.codec)) ::
      ("config" | kafkaMap(kafkaRequiredString, kafkaRequiredString))
    ).as[CreateTopicRequest]
}

final case class TopicResponse(topic: String, kafkaResult: KafkaResult)

object TopicResponse {
  val codec: Codec[TopicResponse] = (("topic" | kafkaRequiredString) :: ("kafkaResult" | KafkaResult.codec)).as[TopicResponse]
}