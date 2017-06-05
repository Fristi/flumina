package flumina

import java.io.File

import com.spotify.docker.client.DefaultDockerClient

import scala.collection.JavaConverters._
import scala.language.postfixOps

object KafkaDocker {

  import sys.process._

  val sleepPerService = 5000l

  def process(file: File)(args: String*) =
    Process(Seq("docker-compose", "-f", file.getAbsolutePath) ++ args, None) !

  def start(file: File) = {
    println(s"There are currently: ${listContainers.size} containers running")

    if (process(file)("up", "-d") != 0) {
      sys.error("Unable to bring docker-compose up")
    } else {
      Thread.sleep(sleepPerService)
    }
  }

  def scaleKafka(file: File, scale: Int) =
    if (process(file)("scale", s"kafka=$scale") != 0) {
      sys.error(s"Unable to scale kafka with $scale")
    } else {
      Thread.sleep(sleepPerService * (scale - 1))
    }

  def stopService(file: File, service: String) =
    if (process(file)("scale", s"$service=0") != 0) sys.error(s"Unable to stop $service")

  def stop(file: File) =
    if (process(file)("down") != 0) sys.error("Unable to stop")

  def remove(file: File) =
    if (process(file)("rm") != 0) sys.error("Unable to remove")

  private def client         = DefaultDockerClient.builder().uri("unix:///var/run/docker.sock").build()
  private def listContainers = client.listContainers().asScala

  def getPortFor(service: String, instance: Int) = {

    val containers = listContainers

    for {
      container <- containers.find(c =>
        c.labels().asScala.exists {
          case (key, value) => key == "com.docker.compose.service" && value == service
        } && c.labels().asScala.exists {
          case (key, value) =>
            key == "com.docker.compose.container-number" && value == instance.toString
      })
      firstPortMapping <- container.ports.asScala.headOption
    } yield firstPortMapping.getPublicPort
  }

}
