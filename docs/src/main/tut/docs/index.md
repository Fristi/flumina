---
layout: docs
title: Getting Started
---


# Getting started

Before you start you'll need the following:

- Kafka 0.10.1.0 or later
- Scala 2.12.1 or later

First things first, we need some imports

```tut:silent
import flumina.akkaimpl._
import flumina.core.ir._
import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext
```


Now let's create one `KafkaClient`. You'll only need one per program:

```tut:silent
implicit val actorSystem: ActorSystem = ActorSystem()
implicit val executionContext: ExecutionContext = actorSystem.dispatcher //use this for now, you might want to replace it withs something else

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

### Create a topic

```tut:silent
kafkaClient.createTopics(
    topics = Set(
        TopicDescriptor(
            topic = "your-topic-name",
            nrPartitions = Some(10),
            replicationFactor = Some(1),
            replicaAssignment = Seq.empty[ReplicationAssignment],
            config = Map.empty[String, String] // you could specify the java properties as seen on the interwebs here
        )
    )
)
```


### Publish a message to a topic

```tut:silent
import scodec.bits.ByteVector

val topicPartition = TopicPartition(topic = "your-topic-name", partition = 0)
val testRecord = Record(key = ByteVector.empty, value = ByteVector("Hello world".getBytes()))

kafkaClient.produceOne(TopicPartitionValue(topicPartition, testRecord))
```


### Read from a topic

```tut:silent
kafkaClient.fetch(Set(TopicPartitionValue(topicPartition, 0l)))
```


This will return a `Future[TopicPartitionValues[List[RecordEntry]]]`. If you want streaming, checkout the streaming sections.

```tut:invisible
actorSystem.terminate()
```

