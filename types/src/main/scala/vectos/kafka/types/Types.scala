package vectos.kafka.types

import scodec._
import scodec.bits.{crc, _}
import scodec.codecs._

/** Common **/

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

  def fromInt(errorCode: Int): Option[KafkaError] = errorCode match {
    case 0 => Some(NoError)
    case -1 => Some(Unknown)
    case 1 => Some(OffsetOutOfRange)
    case 2 => Some(InvalidMessage)
    case 3 => Some(UnknownTopicOrPartition)
    case 4 => Some(InvalidMessageSize)
    case 5 => Some(LeaderNotAvailable)
    case 6 => Some(NotLeaderForPartition)
    case 7 => Some(RequestTimedOut)
    case 8 => Some(BrokerNotAvailable)
    case 9 => Some(ReplicaNotAvailable)
    case 10 => Some(MessageSizeTooLarge)
    case 11 => Some(StaleControllerEpochCode)
    case 12 => Some(OffsetMetadataTooLargeCode)
    case 14 => Some(OffsetsLoadInProgressCode)
    case 15 => Some(ConsumerCoordinatorNotAvailableCode)
    case 16 => Some(NotCoordinatorForConsumerCode)
    case 17 => Some(InvalidTopicException) 
    case 18 => Some(RecordListTooLarge) 
    case 19 => Some(NotEnoughReplicas) 
    case 20 => Some(NotEnoughReplicasAfterAppend) 
    case 21 => Some(InvalidRequiredAcks) 
    case 22 => Some(IllegalGeneration) 
    case 23 => Some(UnconsistentGroupProtocol) 
    case 24 => Some(InvalidGroupId) 
    case 25 => Some(UnknownMemberId) 
    case 26 => Some(InvalidSessionTimeout) 
    case 27 => Some(RebalanceInProgress) 
    case 28 => Some(InvalidCommitOffsetSize) 
    case 29 => Some(TopicAuthorizationFailed) 
    case 30 => Some(GroupAuthorizationFailed) 
    case 31 => Some(ClusterAuthorizationFailed) 
    case _ => None
  }

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

case class ApiMessage(apiKey: Int, message: ByteVector)

object ApiMessage {
  def apiKeyForRequest(f: KafkaRequest): Option[Int] = f match {
    case _ : KafkaRequest.Produce => Some(0)
    case _ : KafkaRequest.Fetch => Some(1)
//    case _ : KafkaRequest.Offsets => Some(2)
    case _ : KafkaRequest.Metadata => Some(3)
//    case _ : KafkaRequest.LeaderAndIsr => Some(4)
//    case _ : KafkaRequest.StopReplica => Some(5)
//    case _ : KafkaRequest.UpdateMetadate => Some(6)
//    case _ : KafkaRequest.ControlledShutdown => Some(7)
//    case _ : KafkaRequest.OffsetCommit => Some(8)
//    case _ : KafkaRequest.OffsetFetch => Some(9)
//    case _ : KafkaRequest.GroupCoordinator => Some(10)
//    case _ : KafkaRequest.JoinGroup => Some(11)
//    case _ : KafkaRequest.Heartbeat => Some(12)
//    case _ : KafkaRequest.LeaveGroup => Some(13)
//    case _ : KafkaRequest.SyncGroup => Some(14)
//    case _ : KafkaRequest.DescribeGroups => Some(15)
//    case _ : KafkaRequest.ListGroups => Some(16)
//    case _ : KafkaRequest.SaslHandshake => Some(17)
//    case _ : KafkaRequest.ApiVersions => Some(18)
    case _ => None
  }
}

case class RequestEnvelope(apiKey: Int, apiVersion: Int, correlationId: Int, clientId: String, request: ByteVector)

object RequestEnvelope {
  implicit val codec = (
    ("apiKey" | int16) ::
      ("apiVersion" | int16) ::
      ("correlationId" | int32) ::
      ("clientId" | kafkaString) ::
      ("request" | bytes)
    ).as[RequestEnvelope]
}

case class ResponseEnvelope(correlationId: Int, response: ByteVector)

object ResponseEnvelope {
  implicit val codec = (("correlationId" | int32) :: ("response" | bytes)).as[ResponseEnvelope]
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
      checksum = crc.crc32(payload)
    } yield checksum ++ payload

    override def sizeBound: SizeBound = SizeBound.unknown

    override def decode(bits: BitVector): Attempt[DecodeResult[Message]] = {
      for {
        crcPayload <- fixedSizeBits(32, bytes).decode(bits)
        crcRemainder = crc.crc32(crcPayload.remainder)

        _ <- if(crcPayload.value.toBitVector == crcRemainder) Attempt.successful(())
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

/** Produce **/

case class ProduceDataPartition(partition: Int, messageSets: Vector[MessageSetEntry])

case class ProduceData(topicName: String, partitions: Vector[ProduceDataPartition])

case class ProducePartitionResponse(partition: Int, errorCode: KafkaError, offset: Long)

case class ProduceTopicResponse(topicName: String, partitions: Vector[ProducePartitionResponse])

object ProduceDataPartition {
  implicit def codec(implicit messageSet: Codec[MessageSetEntry]): Codec[ProduceDataPartition] =
    (("partition" | int32) :: ("message" | variableSizeBytes(int32, vector(messageSet)))).as[ProduceDataPartition]
}

object ProduceData {
  implicit def codec(implicit partition: Codec[ProduceDataPartition]): Codec[ProduceData] =
    (("name" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[ProduceData]
}

object ProducePartitionResponse {
  implicit def codec(implicit kafkaError: Codec[KafkaError]): Codec[ProducePartitionResponse] =
    (("partition" | int32) :: ("errorCode" | kafkaError) :: ("offset" | int64)).as[ProducePartitionResponse]
}

object ProduceTopicResponse {
  implicit def codec(implicit partition: Codec[ProducePartitionResponse]): Codec[ProduceTopicResponse] =
    (("name" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[ProduceTopicResponse]
}

/** Fetch **/

final case class FetchDataPartition(partition: Int, fetchOffset: Long, maxBytes: Int)

final case class FetchData(topic: String, partitions: Vector[FetchDataPartition])

final case class FetchTopicPartitionResponse(partition: Int, errorCode: KafkaError, highWaterMark: Long, messages: Vector[MessageSetEntry])

final case class FetchTopicResponse(topic: String, partitions: Vector[FetchTopicPartitionResponse])

object FetchDataPartition {
  implicit def codec: Codec[FetchDataPartition] =
    (("partition" | int32) :: ("fetch_offset" | int64) :: ("max_bytes" | int32)).as[FetchDataPartition]
}

object FetchData {
  implicit def codec(implicit partition: Codec[FetchDataPartition]): Codec[FetchData] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[FetchData]
}

object FetchTopicPartitionResponse {
  implicit def codec(implicit messageSetEntry: Codec[MessageSetEntry], kafkaError: Codec[KafkaError]): Codec[FetchTopicPartitionResponse] =
    (
      ("partition" | int32) ::
      ("errorCode" | kafkaError) ::
      ("highWaterMark" | int64) ::
      ("messages" | variableSizeBytes(int32, vector(messageSetEntry)))
    ).as[FetchTopicPartitionResponse]
}


object FetchTopicResponse {
  implicit def codec(implicit partition: Codec[FetchTopicPartitionResponse]): Codec[FetchTopicResponse] =
    (("topic" | kafkaString) :: ("partitions" | kafkaArray(partition))).as[FetchTopicResponse]
}



/** Meta data **/
case class MetadataBroker(nodeId: Int, host: String, port: Int)

case class MetadataTopicPartitionMetadata(errorCode: KafkaError, id: Int, leader: Int, replicas: Vector[Int], isr: Vector[Int])

case class MetadataTopicMetadata(topicErrorCode: Int, name: String, partitionMetaData: Vector[MetadataTopicPartitionMetadata])

object MetadataBroker {
  implicit val codec = (("nodeId" | int32) :: ("host" | kafkaString) :: ("port" | int32)).as[MetadataBroker]
}

object MetadataTopicPartitionMetadata {
  implicit def codec(implicit kafkaError: Codec[KafkaError]) = (
    ("topicErrorCode" | kafkaError) ::
      ("id" | int32) ::
      ("leader" | int32) ::
      ("replicaes" | kafkaArray(int32)) ::
      ("isr" | kafkaArray(int32))
    ).as[MetadataTopicPartitionMetadata]
}

object MetadataTopicMetadata {
  implicit def codec(implicit metadata: Codec[MetadataTopicPartitionMetadata]): Codec[MetadataTopicMetadata] =
    (("errorCode" | int16) :: ("name" | kafkaString) :: ("partitions" | kafkaArray(metadata))).as[MetadataTopicMetadata]
}


/** Requests **/

sealed trait KafkaRequest

object KafkaRequest {

  final case class Produce(acks: Int, timeout: Int, topics: Vector[ProduceData]) extends KafkaRequest

  final case class Fetch(replicaId: Int, maxWaitTime: Int, minBytes: Int, topics: Vector[FetchData]) extends KafkaRequest

  final case class Metadata(topics: Vector[String]) extends KafkaRequest


  implicit def produce(implicit topic: Codec[ProduceData]): Codec[Produce] =
    (("acks" | int16) :: ("timeout" | int32) :: ("topics" | kafkaArray(topic))).as[Produce]

  implicit def fetch(implicit topic: Codec[FetchData]): Codec[Fetch] =
    (("replicaId" | int32) :: ("maxWaitTime" | int32) :: ("minBytes" | int32) :: ("topics" | kafkaArray(topic))).as[Fetch]

  implicit def metaData: Codec[Metadata] =
    ("topics" | kafkaArray(kafkaString)).as[Metadata]

  implicit def codec: Codec[KafkaRequest] = Codec.coproduct[KafkaRequest].choice
}

/** Response **/

sealed trait KafkaResponse

object KafkaResponse {


  case class Produce(topics: Vector[ProduceTopicResponse]) extends KafkaResponse

  case class Fetch(throttleTime: Int, topics: Vector[FetchTopicResponse]) extends KafkaResponse

  case class Metadata(brokers: Vector[MetadataBroker], topicMetadata: Vector[MetadataTopicMetadata]) extends KafkaResponse


  implicit def produce(implicit topic: Codec[ProduceTopicResponse]): Codec[Produce] =
    ("topics" | kafkaArray(topic)).as[Produce]

  implicit def fetch(implicit topic: Codec[FetchTopicResponse]): Codec[Fetch] = {
    (("throttleTime" | int32) :: ("topics" | kafkaArray(topic))).as[Fetch]
  }

  implicit def metaData(implicit brokers: Codec[MetadataBroker], metadata: Codec[MetadataTopicMetadata]): Codec[Metadata] =
    (("brokers" | kafkaArray(brokers)) :: ("metadata" | kafkaArray(metadata))).as[Metadata]


  implicit def codec: Codec[KafkaResponse] = Codec.coproduct[KafkaResponse].choice
}

