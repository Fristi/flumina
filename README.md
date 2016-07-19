scala-kafka
===

[![Build Status](https://travis-ci.org/vectos/scala-kafka.svg)](https://travis-ci.org/vectos/scala-kafka)
[![Coverage Status](https://coveralls.io/repos/github/vectos/scala-kafka/badge.svg?branch=master)](https://coveralls.io/github/vectos/scala-kafka?branch=master)

Kafka driver written in pure Scala. Request and response messages and their respective codecs reside in the `types` project. Currently we support version 0 only. This is due testing of the protocol. We plan to support version 1, 2 and future versions (this will be copy-pasting and editing some stuff). 

The `akka` project contains a Akka(-streams) based implementation. 

I think it would be very possible to implement this in Monix or fs2 aswell? Just ping me to get started.
