package vectos.kafka.akkaimpl

import java.io.File

import org.scalatest._

trait KafkaDockerTest extends BeforeAndAfterAll { self: Suite =>
  def kafkaVersion: String
  def kafkaScaling: Int

  private val dockerFile = new File(s"akka/src/test/resources/docker-compose-kafka_$kafkaVersion.yml")

  override def beforeAll() = {
    super.beforeAll()
    KafkaDocker.start(dockerFile)

    if (kafkaScaling > 1) {
      KafkaDocker.scaleKafka(dockerFile, kafkaScaling)
    }
  }

  override def afterAll() = {
    super.afterAll()
    KafkaDocker.stop(dockerFile)
    KafkaDocker.remove(dockerFile)
  }
}