package flumina.akkaimpl

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils

import scala.util.control.NonFatal

object Utils {
  def createTopic(name: String, partitions: Int, replicationFactor: Int, zookeeperPort: Int) = {
    val port = zookeeperPort
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
  }
}
