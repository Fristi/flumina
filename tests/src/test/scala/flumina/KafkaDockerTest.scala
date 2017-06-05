package flumina

import java.io.File

import org.scalatest._

trait KafkaDockerTest extends BeforeAndAfterAll { self: Suite =>
  def kafkaVersion: String
  def kafkaScaling: Int

  private val controlDocker = System.getenv("CONTROL_DOCKER") == "1"
  private val dockerFile    = new File(s"tests/src/test/resources/docker-compose-kafka_$kafkaVersion.yml")

  override def beforeAll() = {
    super.beforeAll()
    if (controlDocker) {
      KafkaDocker.start(dockerFile)

      if (kafkaScaling > 1) {
        KafkaDocker.scaleKafka(dockerFile, kafkaScaling)
      }
    }

  }

  override def afterAll() = {
    super.afterAll()
    if (controlDocker) {
      KafkaDocker.stop(dockerFile)
      KafkaDocker.remove(dockerFile)
    }

  }
}
