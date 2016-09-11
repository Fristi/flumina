package flumina.core

import flumina.core.ir.{KafkaBroker, Record, TopicPartition}

object splitter {

  abstract class RequestSplitter[L[_], I] {
    def split(brokerMap: Map[TopicPartition, KafkaBroker], values: L[I]): Map[KafkaBroker, L[I]]
    def canRetry[O](values: L[I], toRetry: Set[TopicPartition]): L[I]
  }

  object Produce extends RequestSplitter[List, (TopicPartition, Record)] {
    def split(brokerMap: Map[TopicPartition, KafkaBroker], values: List[(TopicPartition, Record)]): Map[KafkaBroker, List[(TopicPartition, Record)]] =
      values
        .groupBy { case (topicPartition, _) => topicPartition }
        .foldLeft(Map.empty[KafkaBroker, List[(TopicPartition, Record)]]) {
          case (acc, (topicPartition, entries)) =>
            acc.updatedValue(brokerMap.getOrElse(topicPartition, KafkaBroker.AnyNode), Nil)(_ ++ entries)
        }

    override def canRetry[O](values: List[(TopicPartition, Record)], toRetry: Set[TopicPartition]): List[(TopicPartition, Record)] = for {
      (topicPartition, record) <- values
      if toRetry.contains(topicPartition)
    } yield topicPartition -> record
  }

  object Fetch extends RequestSplitter[Map[TopicPartition, ?], Long] {
    def split(brokerMap: Map[TopicPartition, KafkaBroker], values: Map[TopicPartition, Long]): Map[KafkaBroker, Map[TopicPartition, Long]] =
      values
        .map { case (tp, msgs) => (brokerMap.getOrElse(tp, KafkaBroker.AnyNode), tp, msgs) }
        .groupBy { case (broker, _, _) => broker }
        .map { case (broker, topicPartitionMessages) => broker -> topicPartitionMessages.map(x => x._2 -> x._3).toMap }

    override def canRetry[O](values: Map[TopicPartition, Long], toRetry: Set[TopicPartition]): Map[TopicPartition, Long] = for {
      (topicPartition, record) <- values
      if toRetry.contains(topicPartition)
    } yield topicPartition -> record
  }
}
