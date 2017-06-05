---
layout: docs
title: Getting started
---

# Getting started

## Imports

First things first, we need some imports

```tut:silent
import flumina.akkaimpl._
import flumina.core.ir._
import akka.actor.ActorSystem
import scala.concurrent.{ExecutionContext, Await}
import scala.concurrent.duration._
import scodec.bits.ByteVector
```


Now let's create one `KafkaClient`. You'll only need one per program:

```tut:silent
implicit val actorSystem: ActorSystem = ActorSystem()
implicit val executionContext: ExecutionContext = actorSystem.dispatcher //use this for now, you might want to replace it withs something else

val topicName = s"topic${System.nanoTime()}"
val kafkaClient = KafkaClient()
```


The default settings connect to localhost:9092, if you would like to change that, add the following (and edit accordingly) the following in the `application.conf` of your application.

```
flumina {
  bootstrap-brokers = [
    { host: "localhost", port: 9092 }
  ]
}
```


Now everything is set up you can use all the methods which are available on the `KafkaClient`.

## Create a topic

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
```

And let's see it's result!

```tut
Await.result(createTopic, 10.seconds)
```


## Publish a message to a topic

Setup some values

```tut:silent
val topicPartition = TopicPartition(topic = topicName, partition = 0)
val testRecord = Record(key = ByteVector.empty, value = ByteVector("Hello world".getBytes()))
```

And let's see it's result!

```tut
Await.result(kafkaClient.produceOne(TopicPartitionValue(topicPartition, testRecord)), 10.seconds)
```


## Read from a topic

```tut
Await.result(kafkaClient.fetch(Set(TopicPartitionValue(topicPartition, 0l))), 10.seconds)
```


This will return a `Future[TopicPartitionValues[List[RecordEntry]]]`. If you want to poll and adjust the offset accordingly, checkout the rest of the documentation.

```tut:invisible
actorSystem.terminate()
```
