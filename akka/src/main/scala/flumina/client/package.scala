package flumina

import akka.util.ByteString
import scodec.bits.BitVector

package object client {

  implicit class EnrichedByteString(val value: ByteString) extends AnyVal {
    def toBitVector: BitVector = BitVector.view(value.asByteBuffer)
  }

  implicit class EnrichedBitVector(val value: BitVector) extends AnyVal {
    def toByteString: ByteString = ByteString(value.toByteBuffer)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit final class AnyOps[A](self: A) {
    @inline
    def ===(other: A): Boolean = self == other
  }
}

