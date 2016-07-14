package vectos.kafka.akkaimpl

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.scaladsl.{BidiFlow, Flow, Framing, Keep, Sink, Source, Tcp}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import akka.util.ByteString
import akka.pattern.pipe
import akka.{Done, NotUsed}
import scodec.Attempt
import vectos.kafka.types.v0.messages.{KafkaRequest, KafkaResponse, RequestEnvelope, ResponseEnvelope}

import scala.annotation.tailrec
import scala.util.{Failure, Success}

class KafkaConnection(settings: KafkaConnection.Settings) extends Actor with ActorLogging {

  private implicit val system = context.system
  private implicit val materializer = ActorMaterializer()
  private implicit val ec = context.dispatcher

  val logging: BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] = {
    // function that takes a string, prints it with some fixed prefix in front and returns the string again
    def logger(prefix: String) = Flow[ByteString].map { chunk =>
      log.info(s"$prefix size: ${chunk.size} bytes -> ${chunk.toByteVector.toHex}")
      chunk
    }

    val inputLogger = logger("[OUT]")
    val outputLogger = logger("[IN]")

    // create BidiFlow with a separate logger function for each of both streams
    BidiFlow.fromFlows(outputLogger, inputLogger)
  }

  private var requests = Map.empty[Int, ActorRef]
  private val connection = Tcp().outgoingConnection(settings.host, settings.port).join(logging)
  private val postOffice = BidiFlow.fromGraph(new KafkaPostOffice)
  private val frame = Framing.simpleFramingProtocol(Int.MaxValue - 4)
  private val protocol = postOffice atop kafkaEnvelopes atop frame
  private val queue = Source.queue[(Int, KafkaRequest)](bufferSize = settings.bufferSize, overflowStrategy = OverflowStrategy.fail)
    .via(protocol.join(connection))
    .toMat(Sink.actorRef(context.self, Done))(Keep.left)
    .run()

  def nextId = {
    @tailrec
    def loop(i: Int): Int = {
      if(requests.keySet.contains(i)) loop(i + 1) else i
    }
    loop(1)
  }
  def receive = {

    case OfferRequest(QueueOfferResult.Enqueued, receiver) => ()
    case OfferRequest(QueueOfferResult.Dropped, receiver) => receiver ! Failure(new Exception("Queue dropped request"))
    case OfferRequest(QueueOfferResult.Failure(err), receiver) => receiver ! Failure(err)
    case OfferRequest(QueueOfferResult.QueueClosed, receiver) => receiver ! Failure(new Exception("Queue was closed"))

    case r: KafkaRequest =>
      val nextCorrelationId = nextId
      val receiver = sender()
      requests += nextCorrelationId -> receiver
      queue.offer(nextCorrelationId -> r).map(s => OfferRequest(s, receiver)) pipeTo self

    case (correlationId: Int, response: KafkaResponse) =>
      requests.get(correlationId).foreach(ref => ref ! Success(response))
      requests -= correlationId
  }

  private def attemptFlow[I, O](f: I => Attempt[O]) =
    Flow[I].flatMapConcat(input => f(input).fold(
      err => {
        println(err.messageWithContext)
        Source.failed(new Exception(err.messageWithContext)) },
      s => Source.single(s))
    )

  private def kafkaEnvelopes: BidiFlow[RequestEnvelope, ByteString, ByteString, ResponseEnvelope, NotUsed] = {
    val read = attemptFlow[ByteString, ResponseEnvelope](x => ResponseEnvelope.codec.decodeValue(x.toByteVector.toBitVector))
    val write = attemptFlow[RequestEnvelope, ByteString](x => RequestEnvelope.codec.encode(x).map(y => y.toByteVector.toByteString))

    BidiFlow.fromFlows(write, read)
  }

  private final case class OfferRequest(queueOfferResult: QueueOfferResult, receiver: ActorRef)
}

object KafkaConnection {
  def props(settings: Settings) = Props(new KafkaConnection(settings))

  final case class Settings(host: String, port: Int, bufferSize: Int)


}