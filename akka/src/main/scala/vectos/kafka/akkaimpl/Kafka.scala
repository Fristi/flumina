package vectos.kafka.akkaimpl

import akka.actor.ActorRef
import akka.util.Timeout

object Kafka {

  final case class Context(
    connection:     ActorRef,
    requestTimeout: Timeout
  )
}

