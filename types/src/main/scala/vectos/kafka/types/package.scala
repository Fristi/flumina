package vectos.kafka

import scodec._
import scodec.codecs._

package object types {

  //TODO: A length of -1 indicates null. string uses an int16 for its size, and bytes uses an int32.
  def kafkaString = variableSizeBytes(int16, ascii)

  //TODO: A length of -1 indicates null. string uses an int16 for its size, and bytes uses an int32.
  def kafkaArray[A](valueCodec: Codec[A]) = vectorOfN(int32, valueCodec)
  def kafkaBytes = variableSizeBytes(int32, bytes)
  def kafkaMessage[A](message: Codec[A]) = variableSizeBytes(int32, message)
}
