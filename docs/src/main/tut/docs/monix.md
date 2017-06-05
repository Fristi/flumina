---
layout: docs
title: Monix integration
---

# Monix integration

## Imports

First things first, we need some imports

```tut:silent
import monix.reactive.{Consumer, Observable, OverflowStrategy}
import monix.execution.Scheduler.Implicits.global
import flumina._
import flumina.client._
import flumina.monix._
import akka.actor._
import akka.util._
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import scodec.bits.ByteVector
import scodec.codecs.uint32
```

After that we'll wire up the client just like in the "Getting started" example:

```tut:silent
implicit val actorSystem: ActorSystem = ActorSystem()
implicit val executionContext: ExecutionContext = actorSystem.dispatcher //use this for now, you might want to replace it withs something else
implicit val timeout: Timeout = 3.seconds


val topicName = s"topic${System.nanoTime()}"
val kafkaClient = KafkaClient()
```

## Create a topic

First lets setup a topic:

```tut:silent
def createTopic = kafkaClient.createTopics(
    topics = Set(
        TopicDescriptor(
            topic = topicName,
            nrPartitions = Some(10),
            replicationFactor = Some(1),
            replicaAssignment = Seq.empty[ReplicationAssignment],
            config = Map.empty[String, String] // you could specify the java properties as seen on the interwebs here
        )
    )
)

Await.result(createTopic, 10.seconds)
```

Now we've imported the monix package we access to two new methods `produce` and `messages`.


## Produce to Kafka

```tut
implicit val longCodec: KafkaCodec[Long] = KafkaCodec.fromValueCodec(uint32)

val producer = kafkaClient.produce[Long](topic = topicName, nrPartitions = 10)

Await.result(Observable.range(0, 100).consumeWith(producer).runAsync, 30.seconds)
```


## Consume from Kafka

```tut
val consumer = Consumer.foldLeft[Long, TopicPartitionValue[OffsetValue[Long]]](0l)((acc, _) => acc + 1)

Await.result(kafkaClient.messages[Long](topicName, ConsumptionStrategy.TerminateEndOfStream).consumeWith(consumer).runAsync, 30.seconds)
```


```tut:invisible
actorSystem.terminate()
```