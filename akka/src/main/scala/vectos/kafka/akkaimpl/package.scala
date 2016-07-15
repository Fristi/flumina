package vectos.kafka

import akka.util.ByteString
import scodec.bits.ByteVector

import scala.language.implicitConversions

package object akkaimpl {

  implicit class EnrichedByteString(val value: ByteString) extends AnyVal {
    def toByteVector: ByteVector = ByteVector.viewAt((idx: Long) => value(idx.toInt), value.size.toLong)
  }

  implicit class EnrichedByteVector(val value: ByteVector) extends AnyVal {
    def toByteString: ByteString = ByteString(value.toByteBuffer)
  }

  implicit class RichMap[K, V](val map: Map[K, V]) {
    def updatedValue(key: K, default: => V)(update: V => V) =
      map.updated(key, update(map.getOrElse(key, default)))

    def updateValues(update: V => V): Map[K, V] = map.map { case (k, v) => k -> update(v) }
  }
}
