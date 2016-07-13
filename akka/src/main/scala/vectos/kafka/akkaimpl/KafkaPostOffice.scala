package vectos.kafka.akkaimpl

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}
import scodec.bits.BitVector
import scodec.{Attempt, Err}
import vectos.kafka.types._

class KafkaPostOffice extends GraphStage[BidiShape[(Int, KafkaRequest), RequestEnvelope, ResponseEnvelope, (Int, KafkaResponse)]] {

  val i1 = Inlet[(Int, KafkaRequest)]("protoIn")
  val o1 = Outlet[RequestEnvelope]("protoOut")
  val i2 = Inlet[ResponseEnvelope]("appIn")
  val o2 = Outlet[(Int, KafkaResponse)]("appOut")

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private var correlationIdsInFlight = Map.empty[Int, BitVector => Attempt[KafkaResponse]]

    setHandler(o1, new OutHandler {
      override def onPull(): Unit = pull(i1)
    })

    setHandler(i1, new InHandler {
      override def onPush() = {
        val (correlationId, req) = grab(i1)
        val requestSetup = responseDecoder(req).flatMap(decoder => apiKeyAndPayload(req).map { case (apiKey, requestPayload) => (apiKey, requestPayload, decoder) })

        requestSetup match {
          case Attempt.Failure(err) =>
            fail(o1, new Exception(err.messageWithContext))
          case Attempt.Successful((apiKey, requestPayload, decoder)) =>
            correlationIdsInFlight += correlationId -> decoder
            push(o1, RequestEnvelope(apiKey, 1, correlationId, "scala-kafka", requestPayload))
        }
      }
    })

    setHandler(i2, new InHandler {
      override def onPush() = {
        val envelope = grab(i2)
        val response = for {
          decoder <- Attempt.fromOption(correlationIdsInFlight.get(envelope.correlationId), Err(s"No handler associated with ${envelope.correlationId}"))
          response <- decoder(envelope.response)
        } yield response

        response match {
          case Attempt.Failure(err) =>
            fail(o2, new Exception(err.messageWithContext))
          case Attempt.Successful(resp) =>
            correlationIdsInFlight -= envelope.correlationId
            push(o2, envelope.correlationId -> resp)
        }
      }
    })

    setHandler(o2, new OutHandler {
      override def onPull(): Unit = pull(i2)
    })
  }

  override def shape = BidiShape(i1, o1, i2, o2)
}