package flumina.benchmark

import java.util.Properties

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.Meter
import flumina.akkaimpl._
import flumina.core.ir._
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import scodec.bits.ByteVector

import scala.concurrent.duration._
import scala.util.control.NonFatal

object Main extends App {

  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val settings = KafkaSettings(
    bootstrapBrokers = Seq(KafkaBroker.Node("localhost", 9092)),
    connectionsPerBroker = 3,
    operationalSettings = KafkaOperationalSettings(
      retryBackoff = 500.milliseconds,
      retryMaxCount = 5,
      fetchMaxBytes = 128 * 1024, // 1mb?
      fetchMaxWaitTime = 5.milliseconds,
      produceTimeout = 1.seconds,
      groupSessionTimeout = 30.seconds,
      heartbeatFrequency = 4,
      consumeAssignmentStrategy = ConsumeAssignmentStrategy.allToLeader
    ),
    requestTimeout = 30.seconds
  )
  val client = KafkaClient(settings)

  val topic = "test3" //Utils.randomTopic(partitions = 10, replicationFactor = 1)

  Utils.createTopic(topic, 10, 1)

  val produceUpstream = new Meter()
  val produceDownstream = new Meter()
  val consumeMeter = new Meter()

  //
  client.consume(groupId = s"somegroup${System.currentTimeMillis()}", TopicPartition.enumerate(topic, nrPartitions = 10))
    .grouped(5000)
    .to(Sink.foreach(_ => consumeMeter.mark(5000)))
  //    .run()

  Source.cycle(() => (1 to 10).iterator)
    .map(x => TopicPartition(topic, x % 10) -> Record(ByteVector.empty, ByteVector((1 to 1).map(_.toByte).toArray)))
    .grouped(500)
    .mapAsyncUnordered(10)(x => client.produce(x.toList))
    .to(Sink.foreach(_ => produceDownstream.mark(500)))
    .run()

  Source.tick(0.seconds, 1.seconds, ())
    .runForeach(_ => println(s"consume: ${consumeMeter.getMeanRate}, produce: (${produceUpstream.getMeanRate}/${produceDownstream.getMeanRate})"))

}

object Utils {

  def createTopic(name: String, partitions: Int, replicationFactor: Int) = {
    val port = 2181
    val zkUtils = ZkUtils(zkUrl = s"localhost:$port", sessionTimeout = 10000, connectionTimeout = 10000, isZkSecurityEnabled = false)

    try {
      AdminUtils.createTopic(
        zkUtils = zkUtils,
        topic = name,
        partitions = partitions,
        replicationFactor = replicationFactor,
        topicConfig = new Properties()
      )
    } catch {
      case NonFatal(e) => e.printStackTrace()
    } finally {
      zkUtils.close()
    }
    Thread.sleep(1000)
  }

  def randomTopic(partitions: Int, replicationFactor: Int): String = {
    val name = s"test${System.nanoTime().toString}"
    createTopic(name, partitions, replicationFactor)
    name
  }
}
