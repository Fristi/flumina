Flumina
===

[![Build Status](https://travis-ci.org/vectos/flumina.svg)](https://travis-ci.org/vectos/flumina)
[![Coverage Status](https://coveralls.io/repos/github/vectos/flumina/badge.svg?branch=master)](https://coveralls.io/github/vectos/flumina?branch=master)
[![Dependencies](https://app.updateimpact.com/badge/762391907245625344/flumina.svg?config=runtime)](https://app.updateimpact.com/latest/762391907245625344/flumina)

Kafka driver written in pure Scala. Request and response messages and their respective codecs reside in the `types` project. Currently we support version 0 only. This is due testing of the protocol. We plan to support version 1, 2 and future versions (this will be copy-pasting and editing some stuff).

## Disclaimer

The library should be treated as alpha software. Be free to try it (clone it and run it), feedback is welcome! Contact me on gitter (@Fristi) or open a issue.

## Setup

```scala
import flumina.types.ir._
import flumina.akkaimpl._

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import scala.concurrent.duration._

implicit val actorSystem = ActorSystem()
implicit val materializer = ActorMaterializer()

val settings = KafkaSettings(
    bootstrapBrokers = Seq(KafkaBroker.Node("localhost", 9092)),
    connectionsPerBroker = 1,
    operationalSettings = KafkaOperationalSettings(
        retryBackoff = 500.milliseconds,
        retryMaxCount = 5,
        fetchMaxBytes = 32 * 1024,
        fetchMaxWaitTime = 20.seconds,
        produceTimeout = 20.seconds,
        groupSessionTimeout = 30.seconds
    ),
    requestTimeout = 30.seconds
)

val client = KafkaClient(settings)

val nrPartitions = 10
val topicName = "test"
```

## Producing values

```scala

//create a Map[TopicPartition, List[Record]], 
//all records grouped by their respective 
//topic and partition. 
val produce = (1 to 10000)
    .map(x => TopicPartition(topicName, x % nrPartitions) -> Record.fromUtf8StringValue(s"Hello world $x"))
    .toMultimap
    
//returns a Future[Result[Long]]
client.produce(produce)
```

## Streaming fetch

```scala
//returns a Source[RecordEntry, NotUsed]
//TODO: return per topicPartition a stream, so they can be processed async
client.fetchFromBeginning(TopicPartition.enumerate(topicName, nrPartitions))
```

### Offsets

TODO