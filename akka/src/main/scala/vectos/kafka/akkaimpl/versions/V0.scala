package vectos.kafka.akkaimpl.versions

import akka.pattern.ask
import cats.data.Xor
import cats.std.future._
import cats.syntax.cartesian._
import scodec.bits.BitVector
import vectos.kafka.akkaimpl._
import vectos.kafka.types._
import vectos.kafka.types.ir._
import vectos.kafka.types.v0._

import scala.concurrent.{ExecutionContext, Future}

final class V0(implicit executionContext: ExecutionContext) extends KafkaAlg[KafkaMonad] {

  private def doRequest[A](g: KafkaContext => KafkaRequest, trace: Boolean)(f: KafkaResponse => Xor[KafkaError, A]): KafkaMonad[A] = {
    def run(retries: Int): KafkaMonad[A] = KafkaMonad.fromFutureXor { ctx =>
      val req = g(ctx)

      for {
        decoder <- responseDecoder(req).toFuture
        (key, payload) <- apiKeyAndPayload(req).toFuture
        respBits <- ctx.connectionPool
          .ask(KafkaBrokerRequest(ctx.broker, KafkaConnectionRequest(apiKey = key, version = 0, requestPayload = payload, trace = trace)))(ctx.requestTimeout)
          .mapTo[BitVector]

        resp <- decoder(respBits).toFuture

        result <- f(resp) match {
          case Xor.Left(KafkaError.Error(result)) if KafkaResult.canRetry(result) && retries < ctx.settings.retryMaxCount =>
            FutureUtils.delay(ctx.settings.retryBackoff) flatMap (_ => run(retries + 1).value.run(ctx))

          case u => Future.successful(u)
        }

      } yield result
    }

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

    doRequest(_ => KafkaRequest.ListOffset(replicaId = -1, topics = topicPartitions), trace = false) {
      case KafkaResponse.ListOffset(topicOffsets) => (for {
        topicOffset <- KafkaList.fromList(topicOffsets)
        topic <- KafkaList.fromOption(topicOffset.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaList.fromList(topicOffset.partitions)
      } yield TopicPartitionResult(TopicPartition(topic, partition.partition), partition.kafkaResult, partition.offsets)).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def offsetFetch(groupId: String, topicPartitions: Set[TopicPartition]) =
    doRequest(_ => KafkaRequest.OffsetFetch(Some(groupId), topicPartitions.groupBy(_.topic).map { case (topic, tp) => OffsetFetchTopicRequest(Some(topic), tp.map(_.partition).toVector) }.toVector), trace = false) {
      case KafkaResponse.OffsetFetch(topics) => (for {
        topic <- KafkaList.fromList(topics)
        topicName <- KafkaList.fromOption(topic.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaList.fromList(topic.partitions)
      } yield TopicPartitionResult(TopicPartition(topicName, partition.partition), partition.kafkaResult, OffsetMetadata(partition.offset, partition.metadata))).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def leaveGroup(group: String, memberId: String) =
    doRequest(_ => KafkaRequest.LeaveGroup(Some(group), Some(memberId)), trace = false) {
      case KafkaResponse.LeaveGroup(kafkaResult) =>
        if (kafkaResult == KafkaResult.NoError) Xor.right(())
        else Xor.left(KafkaError.Error(kafkaResult))
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def offsetCommit(groupId: String, offsets: Map[TopicPartition, OffsetMetadata]) = {
    val offsetTopics = offsets
      .groupBy { case (topicPartition, _) => topicPartition }
      .map {
        case (topicPartition, offset) =>
          OffsetCommitTopicRequest(topic = Some(topicPartition.topic), partitions = offset.values.map(om => OffsetCommitTopicPartitionRequest(topicPartition.partition, om.offset, om.metadata)).toVector)
      }
      .toVector

    doRequest(_ => KafkaRequest.OffsetCommit(Some(groupId), offsetTopics), trace = false) {
      case KafkaResponse.OffsetCommit(topics) => (for {
        topic <- KafkaList.fromList(topics)
        topicName <- KafkaList.fromOption(topic.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaList.fromList(topic.partitions)
      } yield TopicPartitionResult(TopicPartition(topicName, partition.partition), partition.kafkaResult, ())).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def listGroups =
    doRequest(_ => KafkaRequest.ListGroups, trace = false) {
      case KafkaResponse.ListGroups(kafkaResult, groups) =>
        if (kafkaResult == KafkaResult.NoError) {
          (for {
            group <- KafkaList.fromList(groups)
            groupId <- KafkaList.fromOption(group.groupId, KafkaError.MissingInfo("groupId is missing!"))
            protocolType <- KafkaList.fromOption(group.protocolType, KafkaError.MissingInfo("protocolType is missing!"))
          } yield GroupInfo(groupId, protocolType)).run
        } else {
          Xor.left(KafkaError.Error(kafkaResult))
        }
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def syncGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment]) = {
    def makeAssignmentRequest = (for {
      assignment <- KafkaList.fromList(assignments)
      data = MemberAssignmentData(
        version = assignment.memberAssignment.version,
        topicPartition = assignment.memberAssignment.topicPartitions.groupBy(_.topic).map {
          case (topic, partitions) =>
            MemberAssignmentTopicPartitionData(Some(topic), partitions.map(_.partition).toVector)
        }.toVector,
        userData = assignment.memberAssignment.userData.toVector
      )
      assignmentData <- KafkaList.fromAttempt(MemberAssignmentData.codec.encode(data))
    } yield SyncGroupGroupAssignmentRequest(Some(assignment.memberId), assignmentData.toByteArray.toVector)).run

    for {
      assignmentRequest <- KafkaMonad.fromXor(makeAssignmentRequest)
      response <- doRequest(_ => KafkaRequest.SyncGroup(Some(groupId), generationId, Some(memberId), assignmentRequest.toVector), trace = false) {
        case KafkaResponse.SyncGroup(result, assignmentData) =>
          if (result == KafkaResult.NoError) {
            for {
              memberAssignmentData <- MemberAssignmentData.codec.decodeValue(BitVector(assignmentData)).toXor
              memberAssignment <- extractMemberAssignment(memberAssignmentData)
            } yield memberAssignment
          } else {
            Xor.left(KafkaError.Error(result))
          }
        case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
      }
    } yield response
  }

  def fetch(topicPartitionOffsets: Map[TopicPartition, Long]) = {
    def makeRequest(ctx: KafkaContext) = KafkaRequest.Fetch(
      replicaId = -1,
      maxWaitTime = ctx.settings.fetchMaxWaitTime.toMillis.toInt,
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

    doRequest(makeRequest, trace = false) {
      case KafkaResponse.Fetch(topics) =>
        (for {
          topic <- KafkaList.fromList(topics)
          topicName <- KafkaList.fromOption(topic.topicName, KafkaError.MissingInfo("topicName is missing!"))
          partition <- KafkaList.fromList(topic.partitions)
        } yield {
          val topicPartition = TopicPartition(topicName, partition.partition)
          val messages = partition.messages.map(x => RecordEntry(x.offset, Record(x.message.key, x.message.value))).toList
          TopicPartitionResult(topicPartition, partition.kafkaResult, messages)
        }).run

      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def metadata(topics: Set[String]) =
    doRequest(_ => KafkaRequest.Metadata(topics.map(Some.apply).toVector), trace = false) {
      case u: KafkaResponse.Metadata =>
        val brokers = for {
          broker <- KafkaList.fromList(u.brokers)
          brokerHost <- KafkaList.fromOption(broker.host, KafkaError.MissingInfo("host is missing!"))
        } yield Broker(broker.nodeId, brokerHost, broker.port)

        val topicMetadata = for {
          topicMetadata <- KafkaList.fromList(u.topicMetadata)
          topicName <- KafkaList.fromOption(topicMetadata.topicName, KafkaError.MissingInfo("topicName is missing!"))
          partition <- KafkaList.fromList(topicMetadata.partitions)
        } yield TopicPartitionResult(TopicPartition(topicName, partition.id), partition.kafkaResult, TopicInfo(partition.leader, partition.replicas, partition.isr))

        (brokers.run |@| topicMetadata.run).map(Metadata.apply)

      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def joinGroup(groupId: String, protocol: String, protocols: Seq[GroupProtocol]) = {

    def makeGroupProtocolRequest = KafkaMonad.fromXor {
      (for {
        protocol <- KafkaList.fromList(protocols)
        protocolMetadata <- KafkaList.fromList(protocol.consumerProtocol)
        data = ConsumerProtocolMetadataData(
          version = protocolMetadata.version,
          subscriptions = protocolMetadata.subscriptions.map(Some.apply).toVector,
          userData = protocolMetadata.userData.toVector
        )
        metaDataBitVector <- KafkaList.fromAttempt(ConsumerProtocolMetadataData.codec.encode(data))
      } yield JoinGroupProtocolRequest(Some(protocol.protocolName), metaDataBitVector.toByteArray.toVector)).run
    }

    def makeRequest(groupProtocols: Vector[JoinGroupProtocolRequest])(ctx: KafkaContext) = {
      val timeout = ctx.settings.groupSessionTimeout.toMillis.toInt
      KafkaRequest.JoinGroup(
        groupId = Some(groupId),
        sessionTimeOut = timeout,
        memberId = Some(""),
        protocolType = Some(protocol),
        groupProtocols = groupProtocols
      )
    }

    for {
      groupProtocolRequests <- makeGroupProtocolRequest
      response <- doRequest(makeRequest(groupProtocolRequests.toVector), trace = false) {
        case u: KafkaResponse.JoinGroup =>
          def extractMembers(members: Seq[JoinGroupMemberResponse]) = (for {
            group <- KafkaList.fromList(members)
            memberId <- KafkaList.fromOption(group.memberId, KafkaError.MissingInfo("memberId is missing!"))
            protocolMetadata <- KafkaList.fromAttempt(ConsumerProtocolMetadataData.codec.decodeValue(BitVector(group.metadata)))
            consumerProtocolMetadata <- KafkaList.lift(extractConsumerProtocolMetadataData(protocolMetadata))
          } yield GroupMember(memberId, None, None, Some(consumerProtocolMetadata), None)).run

          if (u.kafkaResult == KafkaResult.NoError) {
            for {
              memberId <- Xor.fromOption(u.memberId, KafkaError.MissingInfo("memberId is missing"))
              groupProtocol <- Xor.fromOption(u.groupProtocol, KafkaError.MissingInfo("groupProtocol is missing"))
              leaderId <- Xor.fromOption(u.leaderId, KafkaError.MissingInfo("leaderId is missing"))
              members <- extractMembers(u.members)
            } yield JoinGroupResult(u.generationId, groupProtocol, leaderId, memberId, members)
          } else {
            Xor.left(KafkaError.Error(u.kafkaResult))
          }
        case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
      }
    } yield response

  }

  def produce(values: Map[TopicPartition, List[Record]]) = {
    def makeRequest(ctx: KafkaContext) = KafkaRequest.Produce(
      acks = 1,
      timeout = ctx.settings.produceTimeout.toMillis.toInt,
      topics = values
        .groupBy { case (tp, _) => tp.topic }
        .map {
          case (topic, tpvalues) =>
            ProduceTopicRequest(Some(topic), tpvalues.map {
              case (tp, keyValues) =>
                val messages = keyValues.map { keyedMessage =>
                  MessageSetEntry(offset = 0, message = Message(magicByte = 0, attributes = 0, key = keyedMessage.key.toVector, value = keyedMessage.value.toVector))
                }

                ProduceTopicPartitionRequest(tp.partition, messages.toVector)
            }.toVector)
        }
        .toVector
    )

    doRequest(makeRequest, trace = false) {
      case KafkaResponse.Produce(topics) => (for {
        topic <- KafkaList.fromList(topics)
        topicName <- KafkaList.fromOption(topic.topicName, KafkaError.MissingInfo("topicName is missing!"))
        partition <- KafkaList.fromList(topic.partitions)
      } yield TopicPartitionResult(TopicPartition(topicName, partition.partition), partition.kafkaResult, partition.offset)).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }
  }

  def describeGroups(groupIds: Set[String]) =
    doRequest(_ => KafkaRequest.DescribeGroups(groupIds.map(Some.apply).toVector), trace = false) {
      case KafkaResponse.DescribeGroups(groups) =>
        def groupMembers(members: Seq[DescribeGroupsGroupMemberResponse]) = {
          (for {
            member <- KafkaList.fromList(members)
            memberId <- KafkaList.fromOption(member.memberId, KafkaError.MissingInfo("memberId is missing!"))
            protocolMetadata <- if (member.memberMetadata.nonEmpty) {
              for {
                protocolMetadata <- KafkaList.fromAttempt(ConsumerProtocolMetadataData.codec.decodeValue(BitVector(member.memberMetadata)))
                consumerProtocolMetadata <- KafkaList.lift(extractConsumerProtocolMetadataData(protocolMetadata))
              } yield Some(consumerProtocolMetadata)
            } else {
              KafkaList.lift(Xor.right(None))
            }

            assignment <- if (member.memberAssignment.nonEmpty) {
              for {
                memberAssignmentData <- KafkaList.fromAttempt(MemberAssignmentData.codec.decodeValue(BitVector(member.memberAssignment)))
                memberAssignment <- KafkaList.lift(extractMemberAssignment(memberAssignmentData))
              } yield Some(memberAssignment)
            } else {
              KafkaList.lift(Xor.right(None))
            }

          } yield GroupMember(memberId, member.clientId, member.clientHost, protocolMetadata, assignment)).run
        }

        (for {
          group <- KafkaList.fromList(groups)
          groupName <- KafkaList.fromOption(group.groupId, KafkaError.MissingInfo("groupId is missing!"))
          state <- KafkaList.fromOption(group.state, KafkaError.MissingInfo("state is missing!"))
          protocolType <- KafkaList.fromOption(group.protocolType, KafkaError.MissingInfo("protocolType is missing!"))
          protocol <- KafkaList.fromOption(group.protocol, KafkaError.MissingInfo("protocol is missing!"))
          members <- KafkaList.lift(groupMembers(group.members))
        } yield Group(group.kafkaResult, groupName, state, protocolType, protocol, members)).run
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def heartbeat(group: String, generationId: Int, memberId: String) =
    doRequest(_ => KafkaRequest.Heartbeat(Some(group), generationId, Some(memberId)), trace = false) {
      case KafkaResponse.Heartbeat(kafkaResult) => if (kafkaResult == KafkaResult.NoError) Xor.right(()) else Xor.left(KafkaError.Error(kafkaResult))
      case _                                    => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def groupCoordinator(groupId: String) =
    doRequest(_ => KafkaRequest.GroupCoordinator(Some(groupId)), trace = false) {
      case u: KafkaResponse.GroupCoordinator =>
        if (u.kafkaResult == KafkaResult.NoError) {
          u.coordinatorHost match {
            case Some(host) => Xor.right(Broker(u.coordinatorId, host, u.coordinatorPort))
            case _          => Xor.left(KafkaError.MissingInfo("coordinator host is missing!"))
          }
        } else {
          Xor.left(KafkaError.Error(u.kafkaResult))
        }
      case _ => Xor.left(KafkaError.OtherResponseTypeExpected)
    }

  def pure[A](x: A) = KafkaMonad.pure(x)
  def flatMap[A, B](fa: KafkaMonad[A])(f: (A) => KafkaMonad[B]) = fa.flatMap(f)

  private def extractMemberAssignment(data: MemberAssignmentData) = {
    def extractMemberAssignmentTopicPartition(memberAssignmentTopicPartitions: Seq[MemberAssignmentTopicPartitionData]) = (for {
      matp <- KafkaList.fromList(memberAssignmentTopicPartitions)
      topicName <- KafkaList.fromOption(matp.topicName, KafkaError.MissingInfo("topicName is missing!"))
      partition <- KafkaList.fromList(matp.partitions)
    } yield TopicPartition(topicName, partition)).run

    for {
      matps <- extractMemberAssignmentTopicPartition(data.topicPartition)
    } yield MemberAssignment(data.version, matps, data.userData)
  }

  private def extractConsumerProtocolMetadataData(data: ConsumerProtocolMetadataData) = {
    val subscriptions = (for {
      subscription <- KafkaList.fromList(data.subscriptions)
      subscriptionName <- KafkaList.fromOption(subscription, KafkaError.MissingInfo("subscription is missing!"))
    } yield subscriptionName).run

    subscriptions.map(subs => ConsumerProtocol(data.version, subs, data.userData))
  }
}
