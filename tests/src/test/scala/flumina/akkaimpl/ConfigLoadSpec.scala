package flumina.akkaimpl

import com.typesafe.config.ConfigFactory
import flumina.core.ir.{ KafkaBroker, KafkaOperationalSettings, KafkaSettings }
import org.scalatest._
import pureconfig.loadConfig

import scala.concurrent.duration._

class ConfigLoadSpec extends Suite with WordSpecLike with Matchers {

  "ConfigLoadSpec" should {
    "load" in {
      loadConfig[KafkaSettings](ConfigFactory.load().getConfig("flumina")) shouldBe Right(
        KafkaSettings(
          bootstrapBrokers = KafkaBroker.Node("localhost", 9092) :: Nil,
          operationalSettings = KafkaOperationalSettings(
            retryBackoff = 500.milliseconds,
            retryMaxCount = 5,
            fetchMaxWaitTime = 5.milliseconds,
            fetchMaxBytes = 131072,
            produceTimeout = 1.seconds,
            groupSessionTimeout = 30.seconds
          ),
          connectionsPerBroker = 3,
          requestTimeout = 30.seconds
        )
      )
    }
  }
}