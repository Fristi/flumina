package vectos.kafka.akkaimpl

import akka.actor.ActorSystem
import akka.routing.RoundRobinPool
import akka.stream._
import akka.util.Timeout
import cats.syntax.all._
import vectos.kafka.akkaimpl.versions.V0
import vectos.kafka.types.dsl._
import vectos.kafka.types.ir.TopicPartition

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Main extends App {

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  val context = Kafka.Context(
    connection = system.actorOf(RoundRobinPool(15).props(KafkaConnection.props(KafkaConnection.Settings("localhost", 9092, 1000)))),
    requestTimeout = Timeout(2.seconds)
  )

  val protocol = new V0()

  val res = for {
    _ <- produce(Map(TopicPartition("test", 0) -> List("key".getBytes -> "Hello world".getBytes)))
    _ <- produce(Map(TopicPartition("test", 0) -> List("key".getBytes -> "Hello world".getBytes)))
    listOffsets <- listOffsets(Set(TopicPartition("test", 0)))
  } yield listOffsets

  res(protocol).value.run(context).onComplete(println)

  //  Kafka.v0.listOffsets(Set(TopicPartition("test", 0))).run(context).onComplete(println)

  //
  //  Source.unfold(0)(s => Some(s -> producer))
  //    .via(Kafka.produceSlidingFlow(1000, 50))
  //    .runWith(interleave)
}
