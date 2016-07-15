package vectos.kafka.types.v0.messages

import scodec._
import scodec.codecs._

final case class ListGroupGroupResponse(groupId: Option[String], protocolType: Option[String])

object ListGroupGroupResponse {
  implicit val codec: Codec[ListGroupGroupResponse] = (("groupId" | kafkaString) :: ("protocolType" | kafkaString)).as[ListGroupGroupResponse]
}