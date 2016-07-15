package vectos.kafka.types.v0.messages

import scodec._
import scodec.codecs._

trait ListGroupsTypes {
  final case class ListGroupGroupResponse(groupId: Option[String], protocolType: Option[String])

  object ListGroupGroupResponse {
    implicit val codec = (("groupId" | kafkaString) :: ("protocolType" | kafkaString)).as[ListGroupGroupResponse]
  }
}
