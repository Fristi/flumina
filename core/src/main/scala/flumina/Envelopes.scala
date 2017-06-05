package flumina

import scodec._
import scodec.bits._
import scodec.codecs._

final case class RequestEnvelope(apiKey: Int, apiVersion: Int, correlationId: Int, clientId: Option[String], request: BitVector)

final case class ResponseEnvelope(correlationId: Int, response: BitVector)

object RequestEnvelope {
  val codec: Codec[RequestEnvelope] = (
    ("apiKey" | int16) ::
    ("apiVersion" | int16) ::
    ("correlationId" | int32) ::
    ("clientId" | kafkaOptionalString) ::
    ("request" | scodec.codecs.bits)
  ).as[RequestEnvelope]
}

object ResponseEnvelope {
  val codec: Codec[ResponseEnvelope] = (("correlationId" | int32) :: ("response" | scodec.codecs.bits)).as[ResponseEnvelope]
}