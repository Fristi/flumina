package flumina

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s._
import flumina.core.ir.{KafkaCodec, Record}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import scodec.bits.{BitVector, ByteVector}
import scodec.codecs._
import scodec.{Attempt, Err}

import scala.util.control.NonFatal

package object avro4s {

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def avroCodec[A](topic: String, schemaRegistry: SchemaRegistryClient)(implicit S: ToSchema[A], E: ToRecord[A], D: FromRecord[A]): KafkaCodec[A] = new KafkaCodec[A] {

    private val id = schemaRegistry.register(topic, S())

    private val encoderFactory = EncoderFactory.get
    private val decoderFactory = DecoderFactory.get

    private def write(v: A): Attempt[BitVector] = try {
      val out = new ByteArrayOutputStream
      val encoder = encoderFactory.binaryEncoder(out, null)
      val writer = new GenericDatumWriter[GenericRecord](S.apply())

      writer.write(E(v), encoder)
      encoder.flush()
      Attempt.successful(BitVector(out.toByteArray))
    } catch {
      case NonFatal(t) => Attempt.failure(Err(t.getMessage))
    }

    private def read(id: Int, bitVector: BitVector) = try {
      val schema = schemaRegistry.getByID(id)
      val reader = new GenericDatumReader[GenericRecord](schema)

      val decoder = decoderFactory.binaryDecoder(bitVector.toByteArray, null)

      Attempt.successful(D(reader.read(null, decoder)))
    } catch {
      case NonFatal(t) => Attempt.failure(Err(t.getMessage))
    }

    override def encode(value: A): Attempt[Record] = for {
      bv <- int16.encode(id)
      payload <- write(value)
    } yield Record(ByteVector.empty, (bv ++ payload).toByteVector)

    override def decode(value: Record): Attempt[A] = for {
      identifier <- int16.decode(value.value.toBitVector)
      entity <- read(identifier.value, identifier.remainder)
    } yield entity
  }

}
