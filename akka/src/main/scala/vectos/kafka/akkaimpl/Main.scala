package vectos.kafka.akkaimpl

import akka.actor.ActorSystem
import akka.routing.RoundRobinPool
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import vectos.kafka.types.v0.messages.KafkaResponse

import scala.concurrent.duration._
import scala.util.Random



object Main extends App {

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  implicit val context = Kafka.Context(
    connection = system.actorOf(RoundRobinPool(15).props(KafkaConnection.props(KafkaConnection.Settings("localhost", 9092, 1000)))),
    requestTimeout = Timeout(2.seconds),
    executionContext = system.dispatcher
  )

  def producer = Kafka.TopicPartition("test", 0) -> ("key".getBytes -> "Hello world".getBytes)

  val interleave = Flow[KafkaResponse]
    .collect { case u: KafkaResponse.Produce => u }
    .groupedWithin(Int.MaxValue, 1.second)
    .scan(List.empty[Long]) { case (acc, resp) => resp.last.topics.head.partitions.head.offset +: acc}
    .to(Sink.foreach(s => println(offsetDifference(s))))
    .async


  def offsetDifference(xs: List[Long]): Long = {
    xs.take(2) match {
      case x :: y :: Nil =>
        x - y
      case _ =>
        0l
    }
  }

//
//  Source.unfold(0)(s => Some(s -> producer))
//    .via(Kafka.produceSlidingFlow(1000, 50))
//    .runWith(interleave)
//

  Kafka.listOffsets.onComplete(println)
  Kafka.metadata(Vector("test")).onComplete(println)
  Kafka.groupCoordinator("test").onComplete(println)
}



