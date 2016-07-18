package vectos.kafka.akkaimpl

import vectos.kafka.akkaimpl.versions.V0

class ProtocolVersion0 extends KafkaProtocolTest {
  val protocol = new V0()
  val protocolVersion = 0
}