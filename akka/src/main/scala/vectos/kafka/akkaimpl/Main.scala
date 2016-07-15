package vectos.kafka.akkaimpl

import akka.actor.ActorSystem
import akka.routing.RoundRobinPool
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink}
import akka.util.Timeout

import scala.concurrent.duration._
import Kafka._
import vectos.kafka.types.v0.KafkaResponse

import scala.concurrent.ExecutionContext

object Main extends App {

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val context: Context = Context(
    connection = system.actorOf(RoundRobinPool(15).props(KafkaConnection.props(KafkaConnection.Settings("localhost", 9092, 1000)))),
    requestTimeout = Timeout(2.seconds),
    executionContext = system.dispatcher
  )

  def producer = TopicPartition("test", 0) -> ("key".getBytes -> "Hello world".getBytes)

  //
  //  Source.unfold(0)(s => Some(s -> producer))
  //    .via(Kafka.produceSlidingFlow(1000, 50))
  //    .runWith(interleave)
}
