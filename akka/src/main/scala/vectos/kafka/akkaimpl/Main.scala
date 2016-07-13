package vectos.kafka.akkaimpl

import akka.actor.ActorSystem
import akka.routing.RoundRobinPool
import akka.stream._
import akka.stream.scaladsl.Source
import akka.util.Timeout
import vectos.kafka.types.KafkaRequest

import scala.concurrent.duration._
import scala.util.Random



object Main extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  implicit val context = Kafka.Context(
    connection = system.actorOf(RoundRobinPool(15).props(KafkaConnection.props(KafkaConnection.Settings("localhost", 9092, 1000)))),
    requestTimeout = Timeout(2.seconds),
    executionContext = system.dispatcher
  )

  def producer = Kafka.TopicPartition("test", Random.nextInt(10)) -> ("key".getBytes -> "Hello world".getBytes)

  Source.unfold(0)(s => Some(s -> producer))
    .via(Kafka.produceSlidingFlow())
    .runWith(PerfUtils.sink("produce parallel"))

}



