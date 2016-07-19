package vectos.kafka.akkaimpl.versions

import akka.pattern.ask
import cats.data.{Kleisli, Xor, XorT}
import cats.std.future._
import cats.syntax.cartesian._
import scodec.bits.BitVector
import vectos.kafka.akkaimpl.{Kafka, KafkaConnection}
import vectos.kafka.types._
import vectos.kafka.types.ir._
import vectos.kafka.types.v0._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

final class V0(implicit executionContext: ExecutionContext) extends KafkaAlg[KafkaMonad] {
  private def doRequest[A](g: Kafka.Context => KafkaRequest)(f: KafkaResponse => Xor[KafkaError, A]): XorT[Kleisli[Future, Kafka.Context, ?], KafkaError, A] = {
    def run(retries: Int): KafkaMonad[A] = XorT[Kleisli[Future, Kafka.Context, ?], KafkaError, A](Kleisli { ctx =>
      val req = g(ctx)

      for {
        decoder <- responseDecoder(req).toFuture
        (key, payload) <- apiKeyAndPayload(req).toFuture
        respBits <- ctx.connection
          .ask(KafkaConnection.Request(apiKey = key, version = 0, requestPayload = payload))(ctx.requestTimeout)
          .mapTo[Try[BitVector]]
          .flatMap(Future.fromTry)

        resp <- decoder(respBits).toFuture

        result <- f(resp) match {
          case Xor.Left(KafkaError.Error(result)) if KafkaResult.isRetriable(result) && retries < ctx.settings.retryMaxCount =>
            delay(ctx.settings.retryBackoffMs)(()) flatMap (_ => run(retries + 1).value.run(ctx))

          case u => Future.successful(u)
        }

      } yield result
    })

    run(0)
  }

  def listOffsets(topics: Set[TopicPartition]) = {
    val topicPartitions = topics
      .groupBy(_.topic)
      .map {
        case (topic, tp) =>
          ListOffsetTopicRequest(topic = Some(topic), partitions = tp.map(_.partition).map(partition => ListOffsetTopicPartitionRequest(partition = partition, time = -1, maxNumberOfOffsets = 1)).toVector)
      }
      .toVector

    doRequest(_ => KafkaRequest.ListOffset(replicaId = -1, topics = topicPartitions)) {
      case KafkaResponse.ListOffset(topicOffsets) => (for {
        topicOffset <- KafkaResultList.fromList(topicOffsets)
        topic <- KafkaResultList.fromOption(topicOffset.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaResultList.fromList(topicOffset.partitions)
      } yield TopicPartitionResult(TopicPartition(topic, partition.partition), partition.kafkaResult, partition.offsets)).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def offsetFetch(consumerGroup: String, topicPartitions: Set[TopicPartition]) =
    doRequest(_ => KafkaRequest.OffsetFetch(Some(consumerGroup), topicPartitions.groupBy(_.topic).map { case (topic, tp) => OffsetFetchTopicRequest(Some(topic), tp.map(_.partition).toVector) }.toVector)) {
      case KafkaResponse.OffsetFetch(topics) => (for {
        topic <- KafkaResultList.fromList(topics)
        topicName <- KafkaResultList.fromOption(topic.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaResultList.fromList(topic.partitions)
      } yield TopicPartitionResult(TopicPartition(topicName, partition.partition), partition.kafkaResult, OffsetMetadata(partition.offset, partition.metadata))).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def leaveGroup(group: String, memberId: String) =
    doRequest(_ => KafkaRequest.LeaveGroup(Some(group), Some(memberId))) {
      case KafkaResponse.LeaveGroup(errorCode) =>
        if (errorCode == KafkaResult.NoError) Xor.right(())
        else Xor.left(KafkaError.Error(errorCode))
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def offsetCommit(consumerGroup: String, offsets: Map[TopicPartition, OffsetMetadata]) = {
    val offsetTopics = offsets
      .groupBy { case (topicPartition, _) => topicPartition }
      .map {
        case (topicPartition, offset) =>
          OffsetCommitTopicRequest(topic = Some(topicPartition.topic), partitions = offset.values.map(om => OffsetCommitTopicPartitionRequest(topicPartition.partition, om.offset, om.metadata)).toVector)
      }
      .toVector

    doRequest(_ => KafkaRequest.OffsetCommit(Some(consumerGroup), offsetTopics)) {
      case KafkaResponse.OffsetCommit(topics) => (for {
        topic <- KafkaResultList.fromList(topics)
        topicName <- KafkaResultList.fromOption(topic.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaResultList.fromList(topic.partitions)
      } yield TopicPartitionResult(TopicPartition(topicName, partition.partition), partition.kafkaResult, ())).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def listGroups =
    doRequest(_ => KafkaRequest.ListGroups) {
      case KafkaResponse.ListGroups(errorCode, groups) =>
        if (errorCode == KafkaResult.NoError) {
          (for {
            group <- KafkaResultList.fromList(groups)
            groupId <- KafkaResultList.fromOption(group.groupId, KafkaError.MissingInfo("groupId is missing!"))
            protocolType <- KafkaResultList.fromOption(group.protocolType, KafkaError.MissingInfo("protocolType is missing!"))
          } yield GroupInfo(groupId, protocolType)).run
        } else {
          Xor.left(KafkaError.Error(errorCode))
        }
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def fetch(topicPartitionOffsets: Map[TopicPartition, Long]) = {
    def makeRequest(ctx: Kafka.Context) = KafkaRequest.Fetch(
      replicaId = -1,
      maxWaitTime = ctx.settings.fetchMaxWaitTime,
      minBytes = 1,
      topics =
        topicPartitionOffsets
          .groupBy { case (tp, _) => tp.topic }
          .map {
            case (topic, tpo) =>
              FetchTopicRequest(Some(topic), tpo.map { case (tp, offset) => FetchTopicPartitionRequest(tp.partition, offset, ctx.settings.fetchMaxBytes) }.toVector)
          }
          .toVector
    )

    doRequest(makeRequest) {
      case KafkaResponse.Fetch(topics) =>
        (for {
          topic <- KafkaResultList.fromList(topics)
          topicName <- KafkaResultList.fromOption(topic.topicName, KafkaError.MissingInfo("topicName is missing!"))
          partition <- KafkaResultList.fromList(topic.partitions)
        } yield {
          val topicPartition = TopicPartition(topicName, partition.partition)
          val messages = partition.messages.map(x => MessageEntry(x.offset, x.message.key, x.message.value)).toList
          TopicPartitionResult(topicPartition, partition.kafkaResult, messages)
        }).run

      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def metadata(topics: Vector[String]) =
    doRequest(_ => KafkaRequest.Metadata(topics.map(Some.apply))) {
      case u: KafkaResponse.Metadata =>
        val brokers = for {
          broker <- KafkaResultList.fromList(u.brokers)
          brokerHost <- KafkaResultList.fromOption(broker.host, KafkaError.MissingInfo("host is missing!"))
        } yield Broker(broker.nodeId, brokerHost, broker.port)

        val topicMetadata = for {
          topicMetadata <- KafkaResultList.fromList(u.topicMetadata)
          topicName <- KafkaResultList.fromOption(topicMetadata.topicName, KafkaError.MissingInfo("topicName is missing!"))
          partition <- KafkaResultList.fromList(topicMetadata.partitions)
        } yield TopicPartitionResult(TopicPartition(topicName, partition.id), partition.kafkaResult, TopicInfo(partition.leader, partition.replicas, partition.isr))

        (brokers.run |@| topicMetadata.run).map(Metadata.apply)

      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def joinGroup(groupId: String, protocol: String, protocols: Seq[GroupProtocol]) = {
    def makeRequest(ctx: Kafka.Context) =
      KafkaRequest.JoinGroup(Some(groupId), ctx.settings.groupSessionTimeout, Some(""), Some(protocol), protocols.map(x => JoinGroupProtocolRequest(Some(x.protocolName), x.protocolMetadata)).toVector)

    doRequest(makeRequest) {
      case u: KafkaResponse.JoinGroup =>
        def extractMembers(members: Seq[JoinGroupMemberResponse]) = (for {
          group <- KafkaResultList.fromList(members)
          memberId <- KafkaResultList.fromOption(group.memberId, KafkaError.MissingInfo("memberId is missing!"))
        } yield GroupMember(memberId, group.metadata, None, None, None)).run

        if (u.errorCode == KafkaResult.NoError) {
          for {
            memberId <- Xor.fromOption(u.memberId, KafkaError.MissingInfo("memberId is missing"))
            groupProtocol <- Xor.fromOption(u.groupProtocol, KafkaError.MissingInfo("groupProtocol is missing"))
            leaderId <- Xor.fromOption(u.leaderId, KafkaError.MissingInfo("leaderId is missing"))
            members <- extractMembers(u.members)
          } yield JoinGroupResult(u.generationId, groupProtocol, leaderId, memberId, members)
        } else {
          Xor.left(KafkaError.Error(u.errorCode))
        }
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def produce(values: Map[TopicPartition, List[(Array[Byte], Array[Byte])]]) = {
    def makeRequest(ctx: Kafka.Context) = KafkaRequest.Produce(
      acks = 1,
      timeout = ctx.settings.produceTimeout,
      topics = values
        .groupBy { case (tp, _) => tp.topic }
        .map {
          case (topic, tpvalues) =>
            ProduceTopicRequest(Some(topic), tpvalues.map {
              case (tp, keyValues) =>
                val messages = keyValues.map {
                  case (key, value) =>
                    MessageSetEntry(offset = 0, message = Message(magicByte = 0, attributes = 0, key = key.toVector, value = value.toVector))
                }

                ProduceTopicPartitionRequest(tp.partition, messages.toVector)
            }.toVector)
        }
        .toVector
    )

    doRequest(makeRequest) {
      case KafkaResponse.Produce(topics) => (for {
        topic <- KafkaResultList.fromList(topics)
        topicName <- KafkaResultList.fromOption(topic.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaResultList.fromList(topic.partitions)
      } yield TopicPartitionResult(TopicPartition(topicName, partition.partition), partition.kafkaResult, partition.offset)).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def describeGroups(groupIds: Set[String]) =
    doRequest(_ => KafkaRequest.DescribeGroups(groupIds.map(Some.apply).toVector)) {
      case KafkaResponse.DescribeGroups(groups) =>
        def groupMembers(members: Seq[DescribeGroupsGroupMemberResponse]) = (for {
          member <- KafkaResultList.fromList(members)
          memberId <- KafkaResultList.fromOption(member.memberId, KafkaError.MissingInfo("memberId is missing!"))
        } yield GroupMember(memberId, member.memberMetadata, member.clientId, member.clientHost, Some(member.memberAssignment))).run

        (for {
          group <- KafkaResultList.fromList(groups)
          groupName <- KafkaResultList.fromOption(group.groupId, KafkaError.MissingInfo("groupId is missing!"))
          state <- KafkaResultList.fromOption(group.state, KafkaError.MissingInfo("state is missing!"))
          protocolType <- KafkaResultList.fromOption(group.protocolType, KafkaError.MissingInfo("protocolType is missing!"))
          protocol <- KafkaResultList.fromOption(group.protocol, KafkaError.MissingInfo("protocol is missing!"))
          members <- KafkaResultList.lift(groupMembers(group.members))
        } yield Group(group.errorCode, groupName, state, protocolType, protocol, members)).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def heartbeat(group: String, generationId: Int, memberId: String) =
    doRequest(_ => KafkaRequest.Heartbeat(Some(group), generationId, Some(memberId))) {
      case KafkaResponse.Heartbeat(errorCode) => if (errorCode == KafkaResult.NoError) Xor.right(()) else Xor.left(KafkaError.Error(errorCode))
      case _                                  => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def groupCoordinator(groupId: String) =
    doRequest(_ => KafkaRequest.GroupCoordinator(Some(groupId))) {
      case u: KafkaResponse.GroupCoordinator =>
        if (u.kafkaResult == KafkaResult.NoError) {
          u.coordinatorHost match {
            case Some(host) => Xor.right(GroupCoordinator(u.coordinatorId, host, u.coordinatorPort))
            case _          => Xor.left(KafkaError.MissingInfo("coordinator host is missing!"))
          }
        } else {
          Xor.left(KafkaError.Error(u.kafkaResult))
        }
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def pure[A](x: A): KafkaMonad[A] =
    XorT.right[Kleisli[Future, Kafka.Context, ?], KafkaError, A](Kleisli.pure[Future, Kafka.Context, A](x))

  def flatMap[A, B](fa: KafkaMonad[A])(f: (A) => KafkaMonad[B]): KafkaMonad[B] = fa.flatMap(f)
}
