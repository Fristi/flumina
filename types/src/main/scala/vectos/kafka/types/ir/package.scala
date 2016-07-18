package vectos.kafka.types

/**
 * Created by mark on 18/07/16.
 */
package object ir {

  implicit class RichTopicPartitionResult[T](val topicPartitionResult: Seq[TopicPartitionResult[T]]) {
    def containsError = !topicPartitionResult.forall(_.kafkaResult == KafkaResult.NoError)
  }
}
