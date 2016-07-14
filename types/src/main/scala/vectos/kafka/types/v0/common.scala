package vectos.kafka.types.v0

import scodec.bits.{BitVector, crc}
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}

sealed trait KafkaError

object KafkaError {

  case object NoError extends KafkaError
  case object Unknown extends KafkaError
  case object OffsetOutOfRange extends KafkaError
  case object InvalidMessage extends KafkaError
  case object UnknownTopicOrPartition extends KafkaError
  case object InvalidMessageSize extends KafkaError
  case object LeaderNotAvailable extends KafkaError
  case object NotLeaderForPartition extends KafkaError
  case object RequestTimedOut extends KafkaError
  case object BrokerNotAvailable extends KafkaError
  case object ReplicaNotAvailable extends KafkaError
  case object MessageSizeTooLarge extends KafkaError
  case object StaleControllerEpochCode extends KafkaError
  case object OffsetMetadataTooLargeCode extends KafkaError
  case object OffsetsLoadInProgressCode extends KafkaError
  case object ConsumerCoordinatorNotAvailableCode extends KafkaError
  case object NotCoordinatorForConsumerCode extends KafkaError
  case object InvalidTopicException extends KafkaError
  case object RecordListTooLarge extends KafkaError
  case object NotEnoughReplicas extends KafkaError
  case object NotEnoughReplicasAfterAppend extends KafkaError
  case object InvalidRequiredAcks extends KafkaError
  case object IllegalGeneration extends KafkaError
  case object UnconsistentGroupProtocol extends KafkaError
  case object InvalidGroupId extends KafkaError
  case object UnknownMemberId extends KafkaError
  case object InvalidSessionTimeout extends KafkaError
  case object RebalanceInProgress extends KafkaError
  case object InvalidCommitOffsetSize extends KafkaError
  case object TopicAuthorizationFailed extends KafkaError
  case object GroupAuthorizationFailed extends KafkaError
  case object ClusterAuthorizationFailed extends KafkaError

  implicit val codec = discriminated[KafkaError].by(int16)
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


case class Message(magicByte: Int, attributes: Int, key: Vector[Byte], value: Vector[Byte])

object Message {

  implicit val codec = new Codec[Message] {
    override def encode(value: Message): Attempt[BitVector] = for {
      magicByte <- int8.encode(value.magicByte)
      attributes <- int8.encode(value.attributes)
      key <- kafkaArray(byte).encode(value.key)
      value <- kafkaArray(byte).encode(value.value)
      payload = magicByte ++ attributes ++ key ++ value
    } yield crc.crc32(payload) ++ payload

    override def sizeBound: SizeBound = SizeBound.unknown

    override def decode(bits: BitVector): Attempt[DecodeResult[Message]] = {
      for {
        crcPayload <- fixedSizeBits(32, scodec.codecs.bits).decode(bits)
        crcRemainder = crc.crc32(crcPayload.remainder)

        _ <- if(crcPayload.value == crcRemainder) Attempt.successful(())
        else Attempt.failure(Err(s"Payload checksum: ${crcPayload.value} is not equal to $crcRemainder"))

        magicByte <- int8.decode(crcPayload.remainder)
        attributes <- int8.decode(magicByte.remainder)
        key <- kafkaArray(byte).decode(attributes.remainder)
        value <- kafkaArray(byte).decode(key.remainder)
      } yield {
        DecodeResult(Message(magicByte.value, attributes.value, key.value, value.value), value.remainder)
      }
    }
  }
}

case class MessageSetEntry(offset: Long, message: Message)

object MessageSetEntry {
  implicit def messageSet(implicit message: Codec[Message]): Codec[MessageSetEntry] =
    (("offset" | int64) :: ("message" | variableSizeBytes(int32, message))).as[MessageSetEntry]
}