package flumina

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s._
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}

import scala.util.control.NonFatal

package object avro4s {

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def avroCodec[A](topic: String, schemaRegistry: SchemaRegistryClient)(implicit ToSchema: ToSchema[A], ToRecord: ToRecord[A], FromRecord: FromRecord[A]): Codec[A] = new Codec[A] {

    private val encoderFactory = EncoderFactory.get
    private val decoderFactory = DecoderFactory.get

    private def write(v: A): Attempt[BitVector] =
      try {
        val out     = new ByteArrayOutputStream
        val encoder = encoderFactory.binaryEncoder(out, null)
        val writer  = new GenericDatumWriter[GenericRecord](ToSchema.apply())

        writer.write(ToRecord.apply(v), encoder)
        encoder.flush()
        Attempt.successful(BitVector(out.toByteArray))
      } catch {
        case NonFatal(t) => Attempt.failure(Err(t.getMessage))
      }

    private def read(id: Int, bitVector: BitVector) =
      try {
        val schema = schemaRegistry.getByID(id)
        val reader = new GenericDatumReader[GenericRecord](schema)

        val decoder = decoderFactory.binaryDecoder(bitVector.toByteArray, null)

        Attempt.successful(FromRecord.apply(reader.read(null, decoder)))
      } catch {
        case NonFatal(t) => Attempt.failure(Err(t.getMessage))
      }

    override def encode(value: A): Attempt[BitVector] =
      for {
        magic_byte <- ignore(8).encode(())
        id = schemaRegistry.register(topic, ToSchema.apply())
        bv      <- int32.encode(id)
        payload <- write(value)
      } yield magic_byte ++ bv ++ payload

    override def decode(record: BitVector): Attempt[DecodeResult[A]] =
      for {
        magic_byte <- ignore(8).decode(record)
        identifier <- int32.decode(magic_byte.remainder)
        entity     <- read(identifier.value, identifier.remainder)
      } yield DecodeResult(entity, BitVector.empty)

    override def sizeBound: SizeBound = SizeBound.unknown
  }

}
