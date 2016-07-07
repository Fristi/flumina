package vectos.kafka.akkaimpl

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, FlowShape, OverflowStrategy}
import akka.util.{ByteString, Timeout}
import scodec.{Attempt, Err}
import vectos.kafka.types._

import scala.concurrent.duration._


object Kafka {


  private def attemptFlow[I, O](f: I => Attempt[O]) = {
    def errorHandler(err: Err) = {
      println(err.messageWithContext)
      Nil
    }

    Flow[I].mapConcat { x =>
      f(x).fold(errorHandler, e => e :: Nil)
    }
  }

  private def kafkaMessages: BidiFlow[KafkaRequest, ApiMessage, ByteString, KafkaResponse, NotUsed] = {
    val read = attemptFlow[ByteString, KafkaResponse](x => KafkaResponse.codec.decodeValue(x.toByteVector.toBitVector))
    val write = attemptFlow[KafkaRequest, ApiMessage] { x =>
      for {
        apiKey <- Attempt.fromOption(ApiMessage.apiKeyForRequest(x), Err("No api-key for request!"))
        message <- KafkaRequest.codec.encode(x)
      } yield ApiMessage(apiKey, message.toByteVector)
    }

    BidiFlow.fromFlows(write, read)
  }

  private def kafkaEnvelopes: BidiFlow[RequestEnvelope, ByteString, ByteString, ResponseEnvelope, NotUsed] = {
    val read = attemptFlow[ByteString, ResponseEnvelope](x => ResponseEnvelope.codec.decodeValue(x.toByteVector.toBitVector))
    val write = attemptFlow[RequestEnvelope, ByteString](x => RequestEnvelope.codec.encode(x).map(y => y.toByteVector.toByteString))

    BidiFlow.fromFlows(write, read)
  }

  private val correlationManager: BidiFlow[ApiMessage, RequestEnvelope, ResponseEnvelope, ByteString, NotUsed] = {
    def foldEnvelope(correlationId: Int, msg: ApiMessage) =
      RequestEnvelope(apiKey = msg.apiKey, apiVersion = 1, correlationId = correlationId, clientId = "akka-stream", request = msg.message)

    def unfoldEnvelope(msg: ResponseEnvelope): (Int, ByteString) =
      msg.correlationId -> msg.response.toByteString

    CorrelateStage(foldEnvelope, unfoldEnvelope)
  }

  private val frame =
    Framing.simpleFramingProtocol(Int.MaxValue - 4)

  /**
    * request flow: kafka requests -> correlationId stage -> envelopes -> framing
    * response flow: framing -> envelopes -> correlationId stage -> kafka responses
    */
  val protocol = kafkaMessages atop correlationManager atop kafkaEnvelopes atop frame


  val logging: BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] = {
    // function that takes a string, prints it with some fixed prefix in front and returns the string again
    def logger(prefix: String) = Flow[ByteString].map { chunk =>
      println(s"$prefix size: ${chunk.size} bytes -> ${chunk.toByteVector.toHex}")
      chunk
    }

    val inputLogger = logger("-->")
    val outputLogger = logger("<--")

    // create BidiFlow with a separate logger function for each of both streams
    BidiFlow.fromFlows(outputLogger, inputLogger)
  }

  def connect(host: String, port: Int)
             (implicit system: ActorSystem) =
    protocol.join(Tcp().outgoingConnection(host, port)) //.join(logging)
}



object Main extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(2 seconds)
  implicit val ec = system.dispatcher


  def balancer[In, Out](worker: => Flow[In, Out, Any], connCount: Int): Flow[In, Out, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val balancer = b.add(Balance[In](connCount, waitForAllDownstreams = false))
      val merge = b.add(Merge[Out](connCount))

      for (_ <- 1 to connCount) {
        // for each worker, add an edge from the balancer to the worker, then wire
        // it to the merge element
        balancer ~> worker.async ~> merge
      }

      FlowShape(balancer.in, merge.out)
    })
  }

  val kafka = balancer(Kafka.connect("localhost", 9092), 100)

  val key = "key".getBytes.toVector
  val msg = "Hello world".getBytes.toVector

  val produceReqs = (1 to 200).map(x => KafkaRequest.Produce(1, 20000, Vector(ProduceData("test", Vector(ProduceDataPartition(0, Vector(MessageSetEntry(0, Message(0, 0, key, msg))))))))).toList

  val produceSrc = Source.repeat(KafkaRequest.Produce(1, 20000, Vector(ProduceData("test", Vector(ProduceDataPartition(0, Vector(MessageSetEntry(0, Message(0, 0, key, msg)))))))))

  val fetchReq =  KafkaRequest.Fetch(
              replicaId = -1,
              maxWaitTime = 500,
              minBytes = 1,
              topics = Vector(
                  FetchData(
                    topic = "test",
                    partitions = Vector(FetchDataPartition(partition = 0, fetchOffset = 0, maxBytes = 1048576))
                  )
                )
              )

  val queue = Source.queue[KafkaRequest](200, OverflowStrategy.backpressure)
//    .mapMaterializedValue(x => Future.traverse(produceReqs)(x.offer))
    .mapMaterializedValue(_.offer(fetchReq))


//  Source.cycle()

  val printer = Sink.foreach(println)
  val counter = Flow[KafkaResponse].scan(0) { case (acc, _) => acc + 1}.to(printer)



  kafka.runWith(queue, printer)
}



