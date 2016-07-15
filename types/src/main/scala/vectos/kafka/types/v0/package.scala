package vectos.kafka.types

import scodec._
import scodec.bits.BitVector
import scodec.codecs._

package object v0 {

  val kafkaString: Codec[Option[String]] = new KafkaStringCodec
  val kafkaBytes: Codec[Vector[Byte]] = new KafkaBytes

  def kafkaArray[A](valueCodec: Codec[A]): Codec[Vector[A]] = vectorOfN(int32, valueCodec)

  def responseDecoder(f: KafkaRequest): Attempt[BitVector => Attempt[KafkaResponse]] = f match {
    case _: KafkaRequest.Produce          => Attempt.successful(KafkaResponse.produce.decodeValue)
    case _: KafkaRequest.Fetch            => Attempt.successful(KafkaResponse.fetch.decodeValue)
    case _: KafkaRequest.ListOffset       => Attempt.successful(KafkaResponse.listOffset.decodeValue)
    case _: KafkaRequest.Metadata         => Attempt.successful(KafkaResponse.metaData.decodeValue)
    //    case _ : KafkaRequest.LeaderAndIsr => Some(4)
    //    case _ : KafkaRequest.StopReplica => Some(5)
    //    case _ : KafkaRequest.UpdateMetadate => Some(6)
    //    case _ : KafkaRequest.ControlledShutdown => Some(7)
    //    case _ : KafkaRequest.OffsetCommit => Some(8)
    case _: KafkaRequest.OffsetCommit     => Attempt.successful(KafkaResponse.offsetCommit.decodeValue)
    case _: KafkaRequest.OffsetFetch      => Attempt.successful(KafkaResponse.offsetFetch.decodeValue)
    case _: KafkaRequest.GroupCoordinator => Attempt.successful(KafkaResponse.groupCoordinator.decodeValue)
    case _: KafkaRequest.JoinGroup        => Attempt.successful(KafkaResponse.joinGroup.decodeValue)
    case _: KafkaRequest.Heartbeat        => Attempt.successful(KafkaResponse.heartbeat.decodeValue)
    case _: KafkaRequest.LeaveGroup       => Attempt.successful(KafkaResponse.leaveGroup.decodeValue)
    //    case _ : KafkaRequest.SyncGroup => Some(14)
    case _: KafkaRequest.DescribeGroups   => Attempt.successful(KafkaResponse.describeGroups.decodeValue)
    case KafkaRequest.ListGroups          => Attempt.successful(KafkaResponse.listGroups.decodeValue)
    //    case _ : KafkaRequest.SaslHandshake => Some(17)
    //    case _ : KafkaRequest.ApiVersions => Some(18)
    case _                                => Attempt.failure(Err("No response decoder defined!"))
  }

  def apiKeyAndPayload(f: KafkaRequest): Attempt[(Int, BitVector)] = f match {
    case x: KafkaRequest.Produce          => KafkaRequest.produce.encode(x).map(0 -> _)
    case x: KafkaRequest.Fetch            => KafkaRequest.fetch.encode(x).map(1 -> _)
    case x: KafkaRequest.ListOffset       => KafkaRequest.listOffset.encode(x).map(2 -> _)
    case x: KafkaRequest.Metadata         => KafkaRequest.metaData.encode(x).map(3 -> _)
    //    case _ : KafkaRequest.LeaderAndIsr => Some(4)
    //    case _ : KafkaRequest.StopReplica => Some(5)
    //    case _ : KafkaRequest.UpdateMetadate => Some(6)
    //    case _ : KafkaRequest.ControlledShutdown => Some(7)
    case x: KafkaRequest.OffsetCommit     => KafkaRequest.offsetCommit.encode(x).map(8 -> _)
    case x: KafkaRequest.OffsetFetch      => KafkaRequest.offsetFetch.encode(x).map(9 -> _)
    case x: KafkaRequest.GroupCoordinator => KafkaRequest.groupCoordinator.encode(x).map(10 -> _)
    case x: KafkaRequest.JoinGroup        => KafkaRequest.joinGroup.encode(x).map(11 -> _)
    case x: KafkaRequest.Heartbeat        => KafkaRequest.heartbeat.encode(x).map(12 -> _)
    case x: KafkaRequest.LeaveGroup       => KafkaRequest.leaveGroup.encode(x).map(13 -> _)
    //    case _ : KafkaRequest.SyncGroup => Some(14)
    case x: KafkaRequest.DescribeGroups   => KafkaRequest.describeGroups.encode(x).map(15 -> _)
    case KafkaRequest.ListGroups          => KafkaRequest.listGroups.encode(KafkaRequest.ListGroups).map(16 -> _)
    //    case _ : KafkaRequest.SaslHandshake => Some(17)
    //    case _ : KafkaRequest.ApiVersions => Some(18)
    case _                                => Attempt.failure(Err("No api-key found for this request"))
  }
}
