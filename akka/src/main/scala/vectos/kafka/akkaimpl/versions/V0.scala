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
  private def doRequest(req: KafkaRequest): XorT[Kleisli[Future, Kafka.Context, ?], KafkaError, KafkaResponse] =
    XorT[Kleisli[Future, Kafka.Context, ?], KafkaError, KafkaResponse](Kleisli { ctx =>
      for {
        decoder <- responseDecoder(req).toFuture
        (key, payload) <- apiKeyAndPayload(req).toFuture
        respBits <- ctx.connection
          .ask(KafkaConnection.Request(key, 0, payload))(ctx.requestTimeout)
          .mapTo[Try[BitVector]]
          .flatMap(Future.fromTry)

        resp <- decoder(respBits).toFuture
      } yield Xor.right(resp)
    })

  def listOffsets(topics: Set[TopicPartition]) = {
    val topicPartitions = topics
      .groupBy(_.topic)
      .map {
        case (topic, tp) =>
          ListOffsetTopicRequest(topic = Some(topic), partitions = tp.map(_.partition).map(partition => ListOffsetTopicPartitionRequest(partition = partition, time = -1, maxNumberOfOffsets = 1)).toVector)
      }
      .toVector

    doRequest(KafkaRequest.ListOffset(replicaId = -1, topics = topicPartitions)).mapXor {
      case KafkaResponse.ListOffset(topicOffsets) => (for {
        topicOffset <- KafkaResultList.fromList(topicOffsets)
        topic <- KafkaResultList.fromOption(topicOffset.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaResultList.fromList(topicOffset.partitions)
      } yield TopicPartitionResult(TopicPartition(topic, partition.partition), partition.kafkaResult, partition.offsets)).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def offsetFetch(consumerGroup: String, topicPartitions: Set[TopicPartition]) =
    doRequest(KafkaRequest.OffsetFetch(Some(consumerGroup), topicPartitions.groupBy(_.topic).map { case (topic, tp) => OffsetFetchTopicRequest(Some(topic), tp.map(_.partition).toVector) }.toVector)).mapXor {
      case KafkaResponse.OffsetFetch(topics) => (for {
        topic <- KafkaResultList.fromList(topics)
        topicName <- KafkaResultList.fromOption(topic.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaResultList.fromList(topic.partitions)
      } yield TopicPartitionResult(TopicPartition(topicName, partition.partition), partition.kafkaResult, OffsetMetadata(partition.offset, partition.metadata))).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def leaveGroup(group: String, memberId: String) =
    doRequest(KafkaRequest.LeaveGroup(Some(group), Some(memberId))).mapXor {
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

    doRequest(KafkaRequest.OffsetCommit(Some(consumerGroup), offsetTopics)).mapXor {
      case KafkaResponse.OffsetCommit(topics) => (for {
        topic <- KafkaResultList.fromList(topics)
        topicName <- KafkaResultList.fromOption(topic.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaResultList.fromList(topic.partitions)
      } yield TopicPartitionResult(TopicPartition(topicName, partition.partition), partition.kafkaResult, ())).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def listGroups =
    doRequest(KafkaRequest.ListGroups).mapXor {
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
    val request = KafkaRequest.Fetch(
      replicaId = -1,
      maxWaitTime = 500,
      minBytes = 1,
      topics =
        topicPartitionOffsets
          .groupBy { case (tp, _) => tp.topic }
          .map {
            case (topic, tpo) =>
              FetchTopicRequest(Some(topic), tpo.map { case (tp, offset) => FetchTopicPartitionRequest(tp.partition, offset, 8 * 1024) }.toVector)
          }
          .toVector
    )

    doRequest(request).mapXor {
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
    doRequest(KafkaRequest.Metadata(topics.map(Some.apply))).mapXor {
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

  def joinGroup(groupId: String, protocol: String, protocols: Seq[JoinGroupProtocol]) =
    doRequest(KafkaRequest.JoinGroup(Some(groupId), 30000, Some(""), Some(protocol), protocols.map(x => JoinGroupProtocolRequest(Some(x.protocolName), x.metadata)).toVector)).mapXor {
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

  def produce(values: Map[TopicPartition, List[(Array[Byte], Array[Byte])]]) = {
    val request = KafkaRequest.Produce(
      acks = 1,
      timeout = 20000,
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

    doRequest(request).mapXor {
      case KafkaResponse.Produce(topics) => (for {
        topic <- KafkaResultList.fromList(topics)
        topicName <- KafkaResultList.fromOption(topic.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaResultList.fromList(topic.partitions)
      } yield TopicPartitionResult(TopicPartition(topicName, partition.partition), partition.kafkaResult, partition.offset)).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def describeGroups(groupIds: Set[String]) =
    doRequest(KafkaRequest.DescribeGroups(groupIds.map(Some.apply).toVector)).mapXor {
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
    doRequest(KafkaRequest.Heartbeat(Some(group), generationId, Some(memberId))).mapXor {
      case KafkaResponse.Heartbeat(errorCode) => if (errorCode == KafkaResult.NoError) Xor.right(()) else Xor.left(KafkaError.Error(errorCode))
      case _                                  => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def groupCoordinator(groupId: String) =
    doRequest(KafkaRequest.GroupCoordinator(Some(groupId))).mapXor {
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
