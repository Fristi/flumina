---
layout: docs
title: Kafka type-classes
---

# Kafka type-classes

In flumina we use type classes for two cases

#### Producing

- `KafkaEncoder` for encoding values to a topic
- `KafkaPartitioner` for partitioning values to topic partitions

#### Consuming

- `KafkaDecoder` for decoding values from a topic

## Codecs

Flumina defines a few type classes for encoding and decoding `Record` values. A record is defined as `case class Record(key: ByteVector, value: ByteVector)`. The `ByteVector` type comes from scodec (which is vector of bytes, as the name implies). To decode a `ByteVector` scodec offers several codecs in the `scodec.codecs` package. To come back where we started: the two interfaces for encoding and decoding look like this:

```scala
trait KafkaDecoder[A] {
  def decode(value: Record): Attempt[A]
}

trait KafkaEncoder[A] {
  def encode(value: A): Attempt[Record]
}

trait KafkaCodec[A] extends KafkaEncoder[A] with KafkaDecoder[A]
```

Some notes

- `KafkaDecoder` has a `Functor` cats instance
- `KafkaEncoder` has a `Contravariant` cats instance
- `KafkaCodec` has a `Invariant` cats instance

Also all types have useful combinators on their companion object to make the creation a little easier.

### scodec

Under the hood flumina uses scodec for encoding/decoding binary messages to Kafka. The type classes `KafkaDecoder` and `KafkaEncoder` also use `Attempt` to indicate failure, just like in scodec. You can create `KafkaEncoder`, `KafkaDecoder` and `KafkaCodec` from scodec codecs.

For example

```tut:silent

import flumina._
import scodec.codecs._

implicit val longCodec: KafkaCodec[Long] = KafkaCodec.fromValueCodec(uint32)
```

### Avro4s and confluent schema-registry integration

A commonly used configuration is Kafka, Avro and Confluent's schema-registry. The schema-registry keeps track of all the schema's which can be found in the system.
A nice way to start playing with kafka and schema-registry is the docker image: [fast-data-dev](!http://www.landoop.com/kafka/fast-data-dev/). This environment has a kafka instance, schema-registry instance and a nice UI to inspect the messages.

To use avro4s Kafka codecs with flumina you'll need to bring in the avro4s module: `"flumina" % "codecs-avro4s" %% fluminaVersion`

To actually use avro4s you'll need to import avro4s:

```tut:silent
import flumina._
import flumina.avro4s._
import io.confluent.kafka.schemaregistry.client._
```

Now you can use the actual codecs:

```tut:silent
case class Person(name: String, age: Int)

//TODO: you need to replace this with a `CachedSchemaRegistryClient` in the real world
val schemaRegistryClient = new MockSchemaRegistryClient()

implicit val personCodec: KafkaCodec[Person] = avroCodec[Person]("persons", schemaRegistryClient)
```

## Partitioner

To partition the values to the right partition of a topic we can use the `KafkaPartitioner` type class as defined:

```scala
trait KafkaPartitioner[A] {
  def selectPartition(value: A, nrPartitions: Int): Int
}
```

For the polymorphic value `A` we need to return a partition. For a `Int` implementation it's basically: `value % nrPartitions`.

Note that the `KafkaPartitioner` also a `Contravariant` instance, but also a useful `by` combinator:

```tut:silent
implicit val personPartitioner = KafkaPartitioner.by[Person, String](_.name)
```

There are instances for types `Int`, `Long`, `String`, `UUID` defined on the companion object of `KafkaPartitioner`.