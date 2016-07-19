package vectos.kafka.types

import scodec.Codec
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

  def isRetriable(result: KafkaResult) = result match {
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

}