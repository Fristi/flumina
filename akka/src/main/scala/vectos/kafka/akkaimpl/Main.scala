package vectos.kafka.akkaimpl

import akka.actor.ActorSystem
import akka.routing.RoundRobinPool
import akka.stream._
import akka.stream.scaladsl.Source
import akka.util.Timeout

import scala.concurrent.duration._



object Main extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  implicit val context = Kafka.Context(
    connection = system.actorOf(RoundRobinPool(15).props(KafkaConnection.props(KafkaConnection.Settings("localhost", 9092, 1000)))),
    requestTimeout = Timeout(2 seconds),
    executionContext = system.dispatcher
  )

  Source.repeat(Kafka.TopicPartition("test", 0) -> ("key".getBytes -> "Hello world".getBytes))
    .via(Kafka.produceSlidingFlow())
    .runWith(PerfUtils.sink("produce parallel"))
}



