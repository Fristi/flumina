package vectos.kafka.types

package object ir {

  implicit class RichTopicPartitionResult[T](val topicPartitionResult: Seq[TopicPartitionResult[T]]) {
    def containsError = !topicPartitionResult.forall(_.kafkaResult == KafkaResult.NoError)
  }
}
