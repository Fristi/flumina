package vectos.kafka

import akka.util.ByteString
import scodec.bits.ByteVector

package object akkaimpl {

  implicit class EnrichedByteString(val value: ByteString) extends AnyVal {
    def toByteVector: ByteVector = ByteVector.viewAt((idx: Long) => value(idx.toInt), value.size.toLong)
  }

  implicit class EnrichedByteVector(val value: ByteVector) extends AnyVal {
    def toByteString: ByteString = ByteString(value.toByteBuffer)
  }
}
