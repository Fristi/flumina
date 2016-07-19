package vectos.kafka.akkaimpl

import akka.actor.ActorRef
import akka.util.Timeout

object Kafka {

  final case class Settings(
    retryBackoffMs:      Long,
    retryMaxCount:       Int,
    fetchMaxWaitTime:    Int,
    fetchMaxBytes:       Int,
    produceTimeout:      Int,
    groupSessionTimeout: Int
  )

  final case class Context(
    connection:     ActorRef,
    requestTimeout: Timeout,
    settings:       Settings
  )
}

