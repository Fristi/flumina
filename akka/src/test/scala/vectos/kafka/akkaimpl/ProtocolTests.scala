package vectos.kafka.akkaimpl

import vectos.kafka.akkaimpl.versions._

class KafkaProtocolTestVersion0_0901 extends KafkaProtocolTest {
  def kafkaVersion = "0.9.0.1"
  val protocol = new V0
}

class KafkaProtocolTestVersion0_01000 extends KafkaProtocolTest {
  def kafkaVersion = "0.10.0.0"
  val protocol = new V0
}