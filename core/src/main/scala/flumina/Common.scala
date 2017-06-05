package flumina

import cats.Eq
import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs._

sealed trait KafkaResult

object KafkaResult {

  case object NoError extends KafkaResult
  case object Unknown extends KafkaResult
  case object OffsetOutOfRange extends KafkaResult
  case object InvalidMessage extends KafkaResult
  case object UnknownTopicOrPartition extends KafkaResult
  case object InvalidMessageSize extends KafkaResult
  case object LeaderNotAvailable extends KafkaResult
  case object NotLeaderForPartition extends KafkaResult
  case object RequestTimedOut extends KafkaResult
  case object BrokerNotAvailable extends KafkaResult
  case object ReplicaNotAvailable extends KafkaResult
  case object MessageSizeTooLarge extends KafkaResult
  case object StaleControllerEpochCode extends KafkaResult
  case object OffsetMetadataTooLargeCode extends KafkaResult
  case object OffsetsLoadInProgressCode extends KafkaResult
  case object ConsumerCoordinatorNotAvailableCode extends KafkaResult
  case object NotCoordinatorForConsumerCode extends KafkaResult
  case object InvalidTopicException extends KafkaResult
  case object RecordListTooLarge extends KafkaResult
  case object NotEnoughReplicas extends KafkaResult
  case object NotEnoughReplicasAfterAppend extends KafkaResult
  case object InvalidRequiredAcks extends KafkaResult
  case object IllegalGeneration extends KafkaResult
  case object UnconsistentGroupProtocol extends KafkaResult
  case object InvalidGroupId extends KafkaResult
  case object UnknownMemberId extends KafkaResult
  case object InvalidSessionTimeout extends KafkaResult
  case object RebalanceInProgress extends KafkaResult
  case object InvalidCommitOffsetSize extends KafkaResult
  case object TopicAuthorizationFailed extends KafkaResult
  case object GroupAuthorizationFailed extends KafkaResult
  case object ClusterAuthorizationFailed extends KafkaResult
  case object InvalidTimestamp extends KafkaResult
  case object UnsupportedSASLMechanism extends KafkaResult
  case object IllegalSASLState extends KafkaResult
  case object UnsupportedVersion extends KafkaResult
  case object TopicAlreadyExists extends KafkaResult
  case object InvalidPartitions extends KafkaResult
  case object InvalidReplicationFactor extends KafkaResult
  case object InvalidReplicaAssignment extends KafkaResult
  case object InvalidConfig extends KafkaResult
  case object NotController extends KafkaResult
  case object InvalidRequest extends KafkaResult
  case object UnsupportedForMessageFormat extends KafkaResult

  def canRetry(result: KafkaResult): Boolean = result match {
    case InvalidMessage |
      UnknownTopicOrPartition |
      LeaderNotAvailable |
      NotLeaderForPartition |
      RequestTimedOut |
      OffsetsLoadInProgressCode |
      ConsumerCoordinatorNotAvailableCode |
      NotCoordinatorForConsumerCode |
      NotEnoughReplicas |
      NotEnoughReplicasAfterAppend => true
    case _ => false
  }

  implicit val eq: Eq[KafkaResult] = Eq.fromUniversalEquals[KafkaResult]

  implicit val codec: Codec[KafkaResult] = discriminated[KafkaResult].by(int16)
    .typecase(0, provide(NoError))
    .typecase(-1, provide(Unknown))
    .typecase(1, provide(OffsetOutOfRange))
    .typecase(2, provide(InvalidMessage))
    .typecase(3, provide(UnknownTopicOrPartition))
    .typecase(4, provide(InvalidMessageSize))
    .typecase(5, provide(LeaderNotAvailable))
    .typecase(6, provide(NotLeaderForPartition))
    .typecase(7, provide(RequestTimedOut))
    .typecase(8, provide(BrokerNotAvailable))
    .typecase(9, provide(ReplicaNotAvailable))
    .typecase(10, provide(MessageSizeTooLarge))
    .typecase(11, provide(StaleControllerEpochCode))
    .typecase(12, provide(OffsetMetadataTooLargeCode))
    .typecase(14, provide(OffsetsLoadInProgressCode))
    .typecase(15, provide(ConsumerCoordinatorNotAvailableCode))
    .typecase(16, provide(NotCoordinatorForConsumerCode))
    .typecase(17, provide(InvalidTopicException))
    .typecase(18, provide(RecordListTooLarge))
    .typecase(19, provide(NotEnoughReplicas))
    .typecase(20, provide(NotEnoughReplicasAfterAppend))
    .typecase(21, provide(InvalidRequiredAcks))
    .typecase(22, provide(IllegalGeneration))
    .typecase(23, provide(UnconsistentGroupProtocol))
    .typecase(24, provide(InvalidGroupId))
    .typecase(25, provide(UnknownMemberId))
    .typecase(26, provide(InvalidSessionTimeout))
    .typecase(27, provide(RebalanceInProgress))
    .typecase(28, provide(InvalidCommitOffsetSize))
    .typecase(29, provide(TopicAuthorizationFailed))
    .typecase(30, provide(GroupAuthorizationFailed))
    .typecase(31, provide(ClusterAuthorizationFailed))
    .typecase(32, provide(InvalidTimestamp))
    .typecase(33, provide(UnsupportedSASLMechanism))
    .typecase(34, provide(IllegalSASLState))
    .typecase(35, provide(UnsupportedVersion))
    .typecase(36, provide(TopicAlreadyExists))
    .typecase(37, provide(InvalidPartitions))
    .typecase(38, provide(InvalidReplicationFactor))
    .typecase(39, provide(InvalidReplicaAssignment))
    .typecase(40, provide(InvalidConfig))
    .typecase(41, provide(NotController))
    .typecase(42, provide(InvalidRequest))
    .typecase(43, provide(UnsupportedForMessageFormat))
}

final case class ConsumerProtocolMetadataData(version: Int, subscriptions: Vector[String], userData: ByteVector)
final case class MemberAssignmentTopicPartitionData(topicName: String, partitions: Vector[Int])
final case class MemberAssignmentData(version: Int, topicPartitions: Vector[MemberAssignmentTopicPartitionData], userData: ByteVector)

object ConsumerProtocolMetadataData {
  implicit val codec: Codec[ConsumerProtocolMetadataData] =
    (("version" | int16) :: ("subscriptions" | kafkaArray(kafkaRequiredString)) :: ("userData" | kafkaBytes)).as[ConsumerProtocolMetadataData]
}

object MemberAssignmentTopicPartitionData {
  implicit val codec: Codec[MemberAssignmentTopicPartitionData] =
    (("topicName" | kafkaRequiredString) :: ("partition" | kafkaArray(int32))).as[MemberAssignmentTopicPartitionData]
}

object MemberAssignmentData {
  implicit def codec(implicit topicPartition: Codec[MemberAssignmentTopicPartitionData]): Codec[MemberAssignmentData] =
    (("version" | int32) :: ("partitions" | kafkaArray(topicPartition)) :: ("userData" | kafkaBytes)).as[MemberAssignmentData]
}