package flumina.core.v090

import cats.data.Kleisli
import cats.{MonadError, ~>}
import cats.implicits._
import flumina.core.ir._
import flumina.core._
import scodec.Attempt
import scodec.bits.{BitVector, ByteVector}

import scala.annotation.tailrec

final class BasicInterpreter[F[_]](requestResponse: KafkaBrokerRequest => F[BitVector])(implicit F: MonadError[F, Throwable]) extends (KafkaA ~> Kleisli[F, KafkaContext, ?]) {

  def fromAttempt[A](attempt: Attempt[A]): F[A] = attempt match {
    case Attempt.Successful(s) => F.pure(s)
    case Attempt.Failure(err)  => F.raiseError(new Exception(s"Fail: ${err.messageWithContext}"))
  }

  private def doRequest[A](requestFactory: KafkaContext => KafkaRequest, trace: Boolean)(responseTransformer: PartialFunction[KafkaResponse, F[A]]): Kleisli[F, KafkaContext, A] = Kleisli { ctx =>
    val req = requestFactory(ctx)
    val decoder = decoderFor(req)
    val apiVersion = apiVersionFor(req)
    val apiKey = apiKeyFor(req)

    for {
      requestBits <- fromAttempt(encodeRequest(req))
      request = KafkaConnectionRequest(apiKey = apiKey, version = apiVersion, requestPayload = requestBits, trace = trace)
      responseBits <- requestResponse(KafkaBrokerRequest(ctx.broker, request))
      response <- fromAttempt(decoder(responseBits))
      result <- responseTransformer.applyOrElse(response, (resp: KafkaResponse) => F.raiseError[A](new Exception(s"Unexpected response $resp")))
    } yield result
  }

  private def offsetFetch(groupId: String, topicPartitions: Set[TopicPartition]) =
    doRequest(_ => KafkaRequest.OffsetFetch(groupId, topicPartitions.groupBy(_.topic).map { case (topic, tp) => OffsetFetchTopicRequest(topic, tp.map(_.partition).toVector) }.toVector), trace = false) {
      case KafkaResponse.OffsetFetch(topics) => F.pure {
        TopicPartitionValues.from {
          for {
            topic <- topics
            partition <- topic.partitions
          } yield (partition.kafkaResult, TopicPartition(topic.topicName, partition.partition), OffsetMetadata(partition.offset, partition.metadata))
        }
      }
    }

  private def leaveGroup(group: String, memberId: String) =
    doRequest(_ => KafkaRequest.LeaveGroup(group, memberId), trace = false) {
      case KafkaResponse.LeaveGroup(kafkaResult) => F.pure(if (kafkaResult === KafkaResult.NoError) Right(()) else Left(kafkaResult))
    }

  def offsetCommit(groupId: String, generationId: Int, memberId: String, offsets: Map[TopicPartition, OffsetMetadata]) = {
    val offsetTopics = offsets
      .groupBy { case (topicPartition, _) => topicPartition }
      .map {
        case (topicPartition, offset) =>
          OffsetCommitTopicRequest(topic = topicPartition.topic, partitions = offset.values.map(om => OffsetCommitTopicPartitionRequest(topicPartition.partition, om.offset, om.metadata)).toVector)
      }
      .toVector

    //TODO: set retention time
    doRequest(_ => KafkaRequest.OffsetCommit(groupId, generationId, memberId, -1, offsetTopics), trace = false) {
      case KafkaResponse.OffsetCommit(topics) => F.pure {
        TopicPartitionValues.from {
          for {
            topic <- topics
            partition <- topic.partitions
          } yield (partition.kafkaResult, TopicPartition(topic.topicName, partition.partition), ())
        }
      }
    }
  }

  private def listGroups =
    doRequest(_ => KafkaRequest.ListGroups, trace = false) {
      case KafkaResponse.ListGroups(kafkaResult, groups) =>
        F.pure(if (kafkaResult === KafkaResult.NoError) Right(groups.map(g => GroupInfo(g.groupId, g.protocolType)).toList) else Left(kafkaResult))
    }

  private def syncGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment]) = {
    def makeAssignmentRequest(): F[List[SyncGroupGroupAssignmentRequest]] = {
      def encode(data: MemberAssignmentData) = fromAttempt(MemberAssignmentData.codec.encode(data))
      val entries = assignments.map { assignment =>
        val d = MemberAssignmentData(
          version = assignment.memberAssignment.version,
          topicPartitions = assignment.memberAssignment.topicPartitions.groupBy(_.topic).toVector.map {
            case (topic, partitions) =>
              MemberAssignmentTopicPartitionData(topic, partitions.map(_.partition).toVector)
          },
          userData = assignment.memberAssignment.userData
        )

        assignment.memberId -> d
      }

      entries.toList.foldM(List.empty[SyncGroupGroupAssignmentRequest]) {
        case (acc, (mid, d)) =>
          encode(d).map(bv => acc :+ SyncGroupGroupAssignmentRequest(mid, bv.toByteVector))
      }
    }

    def extractResponse(u: KafkaResponse.SyncGroup): F[KafkaResult Either MemberAssignment] = {
      def decode(assignmentData: ByteVector) =
        fromAttempt(MemberAssignmentData.codec.decodeValue(BitVector(assignmentData)))

      if (u.result === KafkaResult.NoError) {
        decode(u.bytes).map(extractMemberAssignment).map(Right(_))
      } else {
        F.pure(Left(u.result))
      }
    }

    for {
      assignmentRequest <- Kleisli[F, KafkaContext, List[SyncGroupGroupAssignmentRequest]](_ => makeAssignmentRequest())
      response <- doRequest(_ => KafkaRequest.SyncGroup(groupId, generationId, memberId, assignmentRequest.toVector), trace = false) {
        case u: KafkaResponse.SyncGroup => extractResponse(u)
      }
    } yield response
  }
  def fetch(topicPartitionOffsets: Traversable[TopicPartitionValue[Long]]) = {
    def makeRequest(ctx: KafkaContext) = KafkaRequest.Fetch(
      replicaId = -1,
      maxWaitTime = ctx.settings.fetchMaxWaitTime.toMillis.toInt,
      minBytes = 1,
      topics =
        topicPartitionOffsets
          .groupBy { _.topicPartition.topic }
          .map {
            case (topic, tpo) =>
              FetchTopicRequest(topic, tpo.map { m => FetchTopicPartitionRequest(m.topicPartition.partition, m.result, ctx.settings.fetchMaxBytes) }.toVector)
          }
          .toVector
    )

    doRequest(makeRequest, trace = false) {
      case KafkaResponse.Fetch(throttleTime, topics) =>
        F.pure {
          TopicPartitionValues.from {
            for {
              topic <- topics
              partition <- topic.partitions
            } yield {
              val topicPartition = TopicPartition(topic.topicName, partition.partition)

              @tailrec
              def loop(msgs: List[Message], acc: List[RecordEntry]): List[RecordEntry] = msgs match {
                case Message.SingleMessage(offset, _, _, key, value) :: xs =>
                  loop(xs, RecordEntry(offset, Record(key, value)) :: acc)
                case Message.CompressedMessages(_, _, _, _, m) :: xs =>
                  loop(m.toList ++ xs, acc)
                case Nil => acc.reverse
              }

              (partition.kafkaResult, topicPartition, loop(partition.messages.toList, Nil))
            }
          }
        }
    }
  }

  private def metadata(topics: Traversable[String]) = {
    doRequest(_ => KafkaRequest.Metadata(topics.toVector), trace = false) {
      case u: KafkaResponse.Metadata =>
        val brokers = u.brokers.map(broker => Broker(broker.nodeId, broker.host, broker.port))
        val topicsFailed = u.topicMetadata.filterNot(_.kafkaResult == KafkaResult.NoError).map(x => TopicResult(x.topicName, x.kafkaResult)).toSet
        val topics = for {
          validTopic <- u.topicMetadata.filter(_.kafkaResult == KafkaResult.NoError)
          partition <- validTopic.partitions
        } yield TopicPartitionValue(TopicPartition(validTopic.topicName, partition.id), TopicInfo(partition.leader, partition.replicas, partition.isr))

        F.pure(
          Metadata(
            brokers.toSet,
            brokers.find(_.nodeId == u.controllerId).getOrElse(sys.error("Should not happen")),
            topics.toSet,
            topicsFailed
          )
        )
    }
  }

  private def joinGroup(groupId: String, memberId: Option[String], protocol: String, protocols: Seq[GroupProtocol]) = {

    def makeGroupProtocolRequest(): F[List[JoinGroupProtocolRequest]] = {
      def encode(data: ConsumerProtocolMetadataData) = fromAttempt(ConsumerProtocolMetadataData.codec.encode(data))
      val entries = for {
        protocol <- protocols
        cp <- protocol.consumerProtocols
      } yield protocol.protocolName -> ConsumerProtocolMetadataData(cp.version, cp.subscriptions.toVector, cp.userData)

      entries.toList.foldM(List.empty[JoinGroupProtocolRequest]) {
        case (acc, (protocolName, data)) =>
          encode(data).map(bv => acc :+ JoinGroupProtocolRequest(protocolName, bv.toByteVector))
      }
    }

    def makeRequest(groupProtocols: Vector[JoinGroupProtocolRequest])(ctx: KafkaContext) = {
      KafkaRequest.JoinGroup(
        groupId = groupId,
        sessionTimeOut = ctx.settings.groupSessionTimeout.toMillis.toInt,
        memberId = memberId.getOrElse(""),
        protocolType = protocol,
        groupProtocols = groupProtocols
      )
    }

    def extractMembers(members: Seq[JoinGroupMemberResponse]): F[List[GroupMember]] = {
      def decode(bitVector: BitVector) = fromAttempt(ConsumerProtocolMetadataData.codec.decodeValue(bitVector))
      members.toList.foldM(List.empty[GroupMember]) {
        case (acc, r) =>
          decode(BitVector(r.metadata)).map(data => acc :+ GroupMember(r.memberId, None, None, Some(extractConsumerProtocolMetadataData(data)), None))
      }
    }

    def extractResponse(u: KafkaResponse.JoinGroup): F[KafkaResult Either JoinGroupResult] =
      if (u.kafkaResult === KafkaResult.NoError) {
        extractMembers(u.members).map(members => Right(JoinGroupResult(u.generationId, u.groupProtocol, u.leaderId, u.memberId, members)))
      } else {
        F.pure(Left(u.kafkaResult))
      }

    for {
      groupProtocolRequests <- Kleisli[F, KafkaContext, List[JoinGroupProtocolRequest]](ctx => makeGroupProtocolRequest())
      response <- doRequest(makeRequest(groupProtocolRequests.toVector), trace = false) {
        case u: KafkaResponse.JoinGroup => extractResponse(u)
      }
    } yield response
  }

  private def produceOne(msg: TopicPartitionValue[Record]) = {
    def makeRequest(ctx: KafkaContext) = KafkaRequest.Produce(
      acks = 1,
      timeout = ctx.settings.produceTimeout.toMillis.toInt,
      topics = Vector(
        ProduceTopicRequest(
          msg.topicPartition.topic,
          Vector(ProduceTopicPartitionRequest(
            msg.topicPartition.partition,
            Vector(Message.SingleMessage(0, MessageVersion.V0, None, msg.result.key, msg.result.value))
          ))
        )
      )
    )

    doRequest(makeRequest, trace = false) {
      case KafkaResponse.Produce(topics, _) =>
        F.pure {
          TopicPartitionValues.from {
            for {
              topic <- topics
              partition <- topic.partitions
            } yield (partition.kafkaResult, TopicPartition(topic.topicName, partition.partition), partition.offset)
          }
        }
    }
  }

  private def produceN(compression: Compression, values: Traversable[TopicPartitionValue[Record]]) = {
    def makeRequest(ctx: KafkaContext) = KafkaRequest.Produce(
      acks = 1,
      timeout = ctx.settings.produceTimeout.toMillis.toInt,
      topics = values
        .groupBy { _.topicPartition.topic }
        .map {
          case (topic, tpvalues) =>
            ProduceTopicRequest(topic, tpvalues.groupBy(_.topicPartition.partition).map {
              case (partition, keyValues) =>
                val messages = keyValues.map(m => Message.SingleMessage(0, MessageVersion.V0, None, m.result.key, m.result.value)).toVector

                ProduceTopicPartitionRequest(
                  partition,
                  Vector(Message.CompressedMessages(0, MessageVersion.V0, compression, None, messages))
                )
            }.toVector)
        }
        .toVector
    )

    doRequest(makeRequest, trace = false) {
      case KafkaResponse.Produce(topics, _) =>
        F.pure {
          TopicPartitionValues.from {
            for {
              topic <- topics
              partition <- topic.partitions
            } yield (partition.kafkaResult, TopicPartition(topic.topicName, partition.partition), partition.offset)
          }
        }
    }
  }

  private def heartbeat(groupId: String, generationId: Int, memberId: String) =
    doRequest(_ => KafkaRequest.Heartbeat(groupId, generationId, memberId), trace = false) {
      case KafkaResponse.Heartbeat(kafkaResult) => F.pure(if (kafkaResult === KafkaResult.NoError) Right(()) else Left(kafkaResult))
    }

  private def groupCoordinator(groupId: String) =
    doRequest(_ => KafkaRequest.GroupCoordinator(groupId), trace = false) {
      case u: KafkaResponse.GroupCoordinator =>
        F.pure(if (u.kafkaResult === KafkaResult.NoError) Right(Broker(u.coordinatorId, u.coordinatorHost, u.coordinatorPort)) else Left(u.kafkaResult))
    }

  private def toTopicResult(tr: TopicResponse) =
    TopicResult(tr.topic, tr.kafkaResult)

  private def createTopics(topics: Traversable[TopicDescriptor]) = {
    def toCreateTopicRequest(td: TopicDescriptor) = {
      def toReplicaAssignment(ra: ReplicationAssignment) =
        ReplicaAssignment(ra.partitionId, ra.replicas.toVector)

      CreateTopicRequest(td.topic, td.nrPartitions, td.replicationFactor, td.replicaAssignment.map(toReplicaAssignment).toVector, td.config)
    }

    doRequest(_ => KafkaRequest.CreateTopic(topics.map(toCreateTopicRequest).toVector, 8000), trace = false) {
      case u: KafkaResponse.CreateTopic =>
        F.pure(u.result.map(toTopicResult).toList)
    }
  }

  private def deleteTopics(topics: Traversable[String]) = {
    doRequest(_ => KafkaRequest.DeleteTopic(topics.toVector, 8000), trace = false) {
      case u: KafkaResponse.DeleteTopic =>
        F.pure(u.result.map(toTopicResult).toList)
    }
  }

  private def extractMemberAssignment(data: MemberAssignmentData) = {
    def extractMemberAssignmentTopicPartition(memberAssignmentTopicPartitions: Seq[MemberAssignmentTopicPartitionData]) = {
      for {
        matp <- memberAssignmentTopicPartitions
        partition <- matp.partitions
      } yield TopicPartition(matp.topicName, partition)
    }

    MemberAssignment(data.version, extractMemberAssignmentTopicPartition(data.topicPartitions), data.userData)
  }

  private def extractConsumerProtocolMetadataData(data: ConsumerProtocolMetadataData) = {
    ConsumerProtocol(data.version, data.subscriptions, data.userData)
  }

  private def apiVersionFor(request: KafkaRequest): Int = request match {
    case _: KafkaRequest.Produce      => 1
    case _: KafkaRequest.Fetch        => 1
    case _: KafkaRequest.OffsetFetch  => 1
    case _: KafkaRequest.OffsetCommit => 2
    case _: KafkaRequest.Metadata     => 2
    case _                            => 0
  }

  private def apiKeyFor(request: KafkaRequest): Int = request match {
    case _: KafkaRequest.Produce          => 0
    case _: KafkaRequest.Fetch            => 1
    case _: KafkaRequest.Metadata         => 3
    case _: KafkaRequest.OffsetCommit     => 8
    case _: KafkaRequest.OffsetFetch      => 9
    case _: KafkaRequest.GroupCoordinator => 10
    case _: KafkaRequest.JoinGroup        => 11
    case _: KafkaRequest.Heartbeat        => 12
    case _: KafkaRequest.LeaveGroup       => 13
    case _: KafkaRequest.SyncGroup        => 14
    case KafkaRequest.ListGroups          => 16
    case _: KafkaRequest.CreateTopic      => 19
    case _: KafkaRequest.DeleteTopic      => 20
  }

  private def decoderFor(request: KafkaRequest): BitVector => Attempt[KafkaResponse] = request match {
    case _: KafkaRequest.Produce          => KafkaResponse.produce.decodeValue
    case _: KafkaRequest.Fetch            => KafkaResponse.fetch.decodeValue
    case _: KafkaRequest.Metadata         => KafkaResponse.metaData.decodeValue
    case _: KafkaRequest.OffsetCommit     => KafkaResponse.offsetCommit.decodeValue
    case _: KafkaRequest.OffsetFetch      => KafkaResponse.offsetFetch.decodeValue
    case _: KafkaRequest.GroupCoordinator => KafkaResponse.groupCoordinator.decodeValue
    case _: KafkaRequest.JoinGroup        => KafkaResponse.joinGroup.decodeValue
    case _: KafkaRequest.Heartbeat        => KafkaResponse.heartbeat.decodeValue
    case _: KafkaRequest.LeaveGroup       => KafkaResponse.leaveGroup.decodeValue
    case _: KafkaRequest.SyncGroup        => KafkaResponse.syncGroup.decodeValue
    case KafkaRequest.ListGroups          => KafkaResponse.listGroups.decodeValue
    case _: KafkaRequest.CreateTopic      => KafkaResponse.createTopic.decodeValue
    case _: KafkaRequest.DeleteTopic      => KafkaResponse.deleteTopic.decodeValue
  }

  private def encodeRequest(request: KafkaRequest): Attempt[BitVector] = request match {
    case x: KafkaRequest.Produce          => KafkaRequest.produce.encode(x)
    case x: KafkaRequest.Fetch            => KafkaRequest.fetch.encode(x)
    case x: KafkaRequest.Metadata         => KafkaRequest.metaData.encode(x)
    case x: KafkaRequest.OffsetCommit     => KafkaRequest.offsetCommit.encode(x)
    case x: KafkaRequest.OffsetFetch      => KafkaRequest.offsetFetch.encode(x)
    case x: KafkaRequest.GroupCoordinator => KafkaRequest.groupCoordinator.encode(x)
    case x: KafkaRequest.JoinGroup        => KafkaRequest.joinGroup.encode(x)
    case x: KafkaRequest.Heartbeat        => KafkaRequest.heartbeat.encode(x)
    case x: KafkaRequest.LeaveGroup       => KafkaRequest.leaveGroup.encode(x)
    case x: KafkaRequest.SyncGroup        => KafkaRequest.syncGroup.encode(x)
    case KafkaRequest.ListGroups          => KafkaRequest.listGroups.encode(KafkaRequest.ListGroups)
    case x: KafkaRequest.CreateTopic      => KafkaRequest.createTopic.encode(x)
    case x: KafkaRequest.DeleteTopic      => KafkaRequest.deleteTopic.encode(x)
  }

  override def apply[A](fa: KafkaA[A]): Kleisli[F, KafkaContext, A] = fa match {
    case KafkaA.ListGroups                                                     => listGroups
    case KafkaA.OffsetsFetch(groupId, values)                                  => offsetFetch(groupId, values)
    case KafkaA.OffsetsCommit(groupId, generationId, memberId, offsets)        => offsetCommit(groupId, generationId, memberId, offsets)
    case KafkaA.ProduceN(compression, values)                                  => produceN(compression, values)
    case KafkaA.ProduceOne(values)                                             => produceOne(values)
    case KafkaA.JoinGroup(groupId, memberId, protocol, protocols)              => joinGroup(groupId, memberId, protocol, protocols)
    case KafkaA.SynchronizeGroup(groupId, generationId, memberId, assignments) => syncGroup(groupId, generationId, memberId, assignments)
    case KafkaA.Heartbeat(groupId, generationId, memberId)                     => heartbeat(groupId, generationId, memberId)
    case KafkaA.Fetch(values)                                                  => fetch(values)
    case KafkaA.LeaveGroup(groupId, memberId)                                  => leaveGroup(groupId, memberId)
    case KafkaA.GroupCoordinator(groupId)                                      => groupCoordinator(groupId)
    case KafkaA.GetMetadata(topics)                                            => metadata(topics)
    case KafkaA.CreateTopics(topics)                                           => createTopics(topics)
    case KafkaA.DeleteTopics(topics)                                           => deleteTopics(topics)
  }
}
