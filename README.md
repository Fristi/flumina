scala-kafka
===

[![Build Status](https://travis-ci.org/vectos/scala-kafka.svg)](https://travis-ci.org/vectos/scala-kafka)
[![Coverage Status](https://coveralls.io/repos/github/vectos/scala-kafka/badge.svg?branch=master)](https://coveralls.io/github/vectos/scala-kafka?branch=master)

Kafka driver written in pure Scala. Request and response messages and their respective codecs reside in the `types` project. Currently we support version 0 only. This is due testing of the protocol. We plan to support version 1, 2 and future versions (this will be copy-pasting and editing some stuff).

## Disclaimer

The library should be treated as alpha software. Be free to try it (clone it and run it), feedback is welcome! Contact me on gitter (@Fristi) or open a issue.

## Setup

```scala
import vectos.kafka.types.ir._
import vectos.kafka.akkaimpl._

import scala.concurrent.duration._

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
val produce = (1 to 10000)
    .map(x => TopicPartition(topicName, x % nrPartitions) -> Record.fromUtf8StringValue(s"Hello world $x"))
    .groupBy { case (topicPartition, _) => topicPartition }
    .foldLeft(Map.empty[TopicPartition, List[Record]]) {
      case (acc, (tp, msgs)) =>
        acc ++ Map(tp -> msgs.foldLeft(List.empty[Record]) { case (acca, e) => acca :+ e._2 })
    }

//returns a Future[Result[Long]]
client.produce(produce)
```

## Streaming fetch

```scala
//returns a Source[RecordEntry, NotUsed]
client.fetchFromBeginning(TopicPartition.enumerate(topicName, nrPartitions))
```

### Offsets

TODO