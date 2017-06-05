package flumina.messages

import scodec._
import scodec.codecs._
import flumina._

final case class ListGroupGroupResponse(groupId: String, protocolType: String)

object ListGroupGroupResponse {
  val codec: Codec[ListGroupGroupResponse] = (("groupId" | kafkaRequiredString) :: ("protocolType" | kafkaRequiredString)).as[ListGroupGroupResponse]
}