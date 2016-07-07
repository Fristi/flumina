package vectos.kafka.akkaimpl

import akka.stream.scaladsl.BidiFlow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, BidiShape, Inlet, Outlet}

import scala.annotation.tailrec

class CorrelateStage[I1, O1, I2, O2](allocate: (Int, I1) => O1, deallocate: I2 => (Int, O2)) extends GraphStage[BidiShape[I1, O1, I2, O2]] {

  val i1 = Inlet[I1]("protoIn")
  val o1 = Outlet[O1]("protoOut")
  val i2 = Inlet[I2]("appIn")
  val o2 = Outlet[O2]("appOut")



  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private var correlationIdsInFlight = Set.empty[Int]

    def nextId = {
      @tailrec
      def loop(i: Int): Int = {
        if(correlationIdsInFlight.contains(i)) loop(i + 1) else i
      }
      loop(1)
    }

    setHandler(o1, new OutHandler {
      override def onPull(): Unit = pull(i1)
    })

    setHandler(i1, new InHandler {
      override def onPush() = {
        val nextCorrelationId = nextId
        correlationIdsInFlight += nextCorrelationId
        push(o1, allocate(nextCorrelationId, grab(i1)))
      }
    })

    setHandler(i2, new InHandler {
      override def onPush() = {
        val (correlationId, msg) = deallocate(grab(i2))
        correlationIdsInFlight -= correlationId
        push(o2, msg)
      }
    })

    setHandler(o2, new OutHandler {
      override def onPull(): Unit = pull(i2)
    })
  }

  override def shape: BidiShape[I1, O1, I2, O2] = BidiShape(i1, o1, i2, o2)
}

object CorrelateStage {
  def apply[I1, O1, I2, O2](allocate: (Int, I1) => O1, deallocate: I2 => (Int, O2)) =
    BidiFlow.fromGraph(new CorrelateStage[I1, O1, I2, O2](allocate, deallocate))
}
