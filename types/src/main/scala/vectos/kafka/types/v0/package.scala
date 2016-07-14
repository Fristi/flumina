package vectos.kafka.types

import scodec._
import scodec.bits.BitVector
import scodec.codecs._
import vectos.kafka.types.v0.{FetchTypes, ProduceTypes, MetadataTypes, ListOffsetTypes}

trait MessageTypes extends FetchTypes with ProduceTypes with MetadataTypes with ListOffsetTypes

package object v0 extends MessageTypes {

  def responseDecoder(f: KafkaRequest): Attempt[BitVector => Attempt[KafkaResponse]] = f match {
    case _ : KafkaRequest.Produce => Attempt.successful(KafkaResponse.produce.decodeValue)
    case _ : KafkaRequest.Fetch => Attempt.successful(KafkaResponse.fetch.decodeValue)
    case _ : KafkaRequest.ListOffset => Attempt.successful(KafkaResponse.listOffset.decodeValue)
    case _ : KafkaRequest.Metadata => Attempt.successful(KafkaResponse.metaData.decodeValue)
    //    case _ : KafkaRequest.LeaderAndIsr => Some(4)
    //    case _ : KafkaRequest.StopReplica => Some(5)
    //    case _ : KafkaRequest.UpdateMetadate => Some(6)
    //    case _ : KafkaRequest.ControlledShutdown => Some(7)
    //    case _ : KafkaRequest.OffsetCommit => Some(8)
    //    case _ : KafkaRequest.OffsetFetch => Some(9)
    case _ : KafkaRequest.GroupCoordinator => Attempt.successful(KafkaResponse.groupCoordinator.decodeValue)
    //    case _ : KafkaRequest.JoinGroup => Some(11)
    //    case _ : KafkaRequest.Heartbeat => Some(12)
    //    case _ : KafkaRequest.LeaveGroup => Some(13)
    //    case _ : KafkaRequest.SyncGroup => Some(14)
    //    case _ : KafkaRequest.DescribeGroups => Some(15)
    //    case _ : KafkaRequest.ListGroups => Some(16)
    //    case _ : KafkaRequest.SaslHandshake => Some(17)
    //    case _ : KafkaRequest.ApiVersions => Some(18)
    case _ => Attempt.failure(Err("No response decoder defined!"))
  }

  def apiKeyAndPayload(f: KafkaRequest): Attempt[(Int, BitVector)] = f match {
    case x : KafkaRequest.Produce => KafkaRequest.produce.encode(x).map(0 -> _)
    case x : KafkaRequest.Fetch => KafkaRequest.fetch.encode(x).map(1 -> _)
    case x : KafkaRequest.ListOffset => KafkaRequest.listOffset.encode(x).map(2 -> _)
    case x : KafkaRequest.Metadata => KafkaRequest.metaData.encode(x).map(3 -> _)
    //    case _ : KafkaRequest.LeaderAndIsr => Some(4)
    //    case _ : KafkaRequest.StopReplica => Some(5)
    //    case _ : KafkaRequest.UpdateMetadate => Some(6)
    //    case _ : KafkaRequest.ControlledShutdown => Some(7)
    //    case _ : KafkaRequest.OffsetCommit => Some(8)
    //    case _ : KafkaRequest.OffsetFetch => Some(9)
    case x : KafkaRequest.GroupCoordinator => KafkaRequest.groupCoordinator.encode(x).map(10 -> _)
    //    case _ : KafkaRequest.JoinGroup => Some(11)
    //    case _ : KafkaRequest.Heartbeat => Some(12)
    //    case _ : KafkaRequest.LeaveGroup => Some(13)
    //    case _ : KafkaRequest.SyncGroup => Some(14)
    //    case _ : KafkaRequest.DescribeGroups => Some(15)
    //    case _ : KafkaRequest.ListGroups => Some(16)
    //    case _ : KafkaRequest.SaslHandshake => Some(17)
    //    case _ : KafkaRequest.ApiVersions => Some(18)
    case _ => Attempt.failure(Err("No api-key found for this request"))
  }


  //TODO: A length of -1 indicates null. string uses an int16 for its size, and bytes uses an int32.
  def kafkaString = variableSizeBytes(int16, ascii)

  //TODO: A length of -1 indicates null. string uses an int16 for its size, and bytes uses an int32.
  def kafkaArray[A](valueCodec: Codec[A]) = vectorOfN(int32, valueCodec)
  def kafkaBytes = variableSizeBytes(int32, bytes)
  def kafkaMessage[A](message: Codec[A]) = variableSizeBytes(int32, message)
}
