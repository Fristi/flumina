package vectos.kafka.akkaimpl

import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.duration._

object PerfUtils {


  def sink[I](prefix: String) = {
    Flow[I]
      .batch(Long.MaxValue, _ => 0)((acc, _) => acc + 1)
      .zip(Source.tick(1.second, 1.second, ()))
      .to(Sink.foreach { case (count, _) => println(s"$prefix | $count msg/s") })
  }
}
