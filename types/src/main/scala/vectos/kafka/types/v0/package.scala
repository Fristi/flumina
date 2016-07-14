package vectos.kafka.types

import scodec._
import scodec.bits.BitVector
import scodec.codecs._
import vectos.kafka.types.v0.messages._

trait MessageTypes extends FetchTypes
  with ProduceTypes
  with MetadataTypes
  with ListOffsetTypes
  with OffsetFetchTypes
  with OffsetCommitTypes
  with JoinGroupTypes

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
    case _ : KafkaRequest.OffsetCommit => Attempt.successful(KafkaResponse.offsetCommit.decodeValue)
    case _ : KafkaRequest.OffsetFetch => Attempt.successful(KafkaResponse.offsetFetch.decodeValue)
    case _ : KafkaRequest.GroupCoordinator => Attempt.successful(KafkaResponse.groupCoordinator.decodeValue)
    case _ : KafkaRequest.JoinGroup => Attempt.successful(KafkaResponse.joinGroup.decodeValue)
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
    case x : KafkaRequest.OffsetCommit => KafkaRequest.offsetCommit.encode(x).map(8 -> _)
    case x : KafkaRequest.OffsetFetch => KafkaRequest.offsetFetch.encode(x).map(9 -> _)
    case x : KafkaRequest.GroupCoordinator => KafkaRequest.groupCoordinator.encode(x).map(10 -> _)
    case x : KafkaRequest.JoinGroup => KafkaRequest.joinGroup.encode(x).map(11 -> _)
    //    case _ : KafkaRequest.Heartbeat => Some(12)
    //    case _ : KafkaRequest.LeaveGroup => Some(13)
    //    case _ : KafkaRequest.SyncGroup => Some(14)
    //    case _ : KafkaRequest.DescribeGroups => Some(15)
    //    case _ : KafkaRequest.ListGroups => Some(16)
    //    case _ : KafkaRequest.SaslHandshake => Some(17)
    //    case _ : KafkaRequest.ApiVersions => Some(18)
    case _ => Attempt.failure(Err("No api-key found for this request"))
  }


  private class KafkaStringCodec extends Codec[Option[String]] {
    val codec = variableSizeBytes(int16, ascii)

    override def decode(bits: BitVector): Attempt[DecodeResult[Option[String]]] = for {
      size <- int16.decode(bits)
      str <- if(size.value == -1) Attempt.successful(DecodeResult(None, size.remainder))
      else ascii.decode(size.remainder).map(_.map(Some.apply))
    } yield str

    override def encode(value: Option[String]): Attempt[BitVector] = value match {
      case Some(str) => codec.encode(str)
      case None => Attempt.successful(BitVector(-1))
    }

    override def sizeBound: SizeBound = codec.sizeBound
  }

  private class KafkaArrayCodec[A](valueCodec: Codec[A]) extends Codec[Vector[A]] {
    val codec = vectorOfN(int32, valueCodec)

    override def decode(bits: BitVector): Attempt[DecodeResult[Vector[A]]] = for {
      size <- int32.decode(bits)
      xs <- if(size.value == -1) Attempt.successful(DecodeResult(Vector.empty[A], size.remainder))
      else vectorOfN(provide(size.value), valueCodec).decode(size.remainder)
    } yield xs

    override def encode(value: Vector[A]): Attempt[BitVector] =
      if(value.isEmpty) Attempt.successful(BitVector(-1))
      else codec.encode(value)

    override def sizeBound: SizeBound = codec.sizeBound
  }

  val kafkaString: Codec[Option[String]] = new KafkaStringCodec
  def kafkaArray[A](valueCodec: Codec[A]): Codec[Vector[A]] = new KafkaArrayCodec(valueCodec)
}
