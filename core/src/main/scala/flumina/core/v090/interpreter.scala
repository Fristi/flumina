package flumina.core.v090

import cats.data.{Ior, Xor}
import cats.implicits._
import cats.kernel.Semigroup
import flumina.core._
import flumina.core.ir._
import scodec.Attempt
import scodec.bits.{BitVector, ByteVector}

final class V090[F[_]](requestResponse: KafkaBrokerRequest => F[BitVector])(implicit F: Async[F]) extends KafkaAlg[KafkaReader[F, ?]] {

  private def doRequest[A](requestFactory: KafkaContext => KafkaRequest, trace: Boolean)(responseTransformer: PartialFunction[KafkaResponse, F[A]]): KafkaReader[F, A] = KafkaReader.fromAsync { ctx =>
    val req = requestFactory(ctx)
    val decoder = decoderFor(req)
    val apiVersion = apiVersionFor(req)
    val apiKey = apiKeyFor(req)

    for {
      requestBits <- Async.fromAttempt(encodeRequest(req))
      request = KafkaConnectionRequest(apiKey = apiKey, version = apiVersion, requestPayload = requestBits, trace = trace)
      responseBits <- requestResponse(KafkaBrokerRequest(ctx.broker, request))
      response <- Async.fromAttempt(decoder(responseBits))
      result <- responseTransformer.applyOrElse(response, (resp: KafkaResponse) => F.fail[A](new Exception(s"Unexpected response $resp")))
    } yield result
  }

  def offsetFetch(groupId: String, topicPartitions: Set[TopicPartition]) =
    doRequest(_ => KafkaRequest.OffsetFetch(groupId, topicPartitions.groupBy(_.topic).map { case (topic, tp) => OffsetFetchTopicRequest(topic, tp.map(_.partition).toVector) }.toVector), trace = false) {
      case KafkaResponse.OffsetFetch(topics) => F.pure {
        TopicPartitionResults.from {
          for {
            topic <- topics
            partition <- topic.partitions
          } yield (partition.kafkaResult, TopicPartition(topic.topicName, partition.partition), OffsetMetadata(partition.offset, partition.metadata))
        }
      }
    }

  def leaveGroup(group: String, memberId: String) =
    doRequest(_ => KafkaRequest.LeaveGroup(group, memberId), trace = false) {
      case KafkaResponse.LeaveGroup(kafkaResult) => F.pure(if (kafkaResult === KafkaResult.NoError) Xor.right(()) else Xor.left(kafkaResult))
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
        TopicPartitionResults.from {
          for {
            topic <- topics
            partition <- topic.partitions
          } yield (partition.kafkaResult, TopicPartition(topic.topicName, partition.partition), ())
        }
      }
    }
  }

  def listGroups =
    doRequest(_ => KafkaRequest.ListGroups, trace = false) {
      case KafkaResponse.ListGroups(kafkaResult, groups) =>
        F.pure(if (kafkaResult === KafkaResult.NoError) Xor.right(groups.map(g => GroupInfo(g.groupId, g.protocolType)).toList) else Xor.left(kafkaResult))
    }

  def syncGroup(groupId: String, generationId: Int, memberId: String, assignments: Seq[GroupAssignment]) = {
    def makeAssignmentRequest(): F[List[SyncGroupGroupAssignmentRequest]] = {
      def encode(data: MemberAssignmentData) = Async.fromAttempt(MemberAssignmentData.codec.encode(data))
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

    def extractResponse(u: KafkaResponse.SyncGroup): F[KafkaResult Xor MemberAssignment] = {
      def decode(assignmentData: ByteVector) =
        Async.fromAttempt(MemberAssignmentData.codec.decodeValue(BitVector(assignmentData)))

      if (u.result === KafkaResult.NoError) {
        decode(u.bytes).map(extractMemberAssignment).map(Xor.right)
      } else {
        F.pure(Xor.left(u.result))
      }
    }

    for {
      assignmentRequest <- KafkaReader.fromAsync[F, List[SyncGroupGroupAssignmentRequest]](_ => makeAssignmentRequest())
      response <- doRequest(_ => KafkaRequest.SyncGroup(groupId, generationId, memberId, assignmentRequest.toVector), trace = false) {
        case u: KafkaResponse.SyncGroup => extractResponse(u)
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
              FetchTopicRequest(topic, tpo.map { case (tp, offset) => FetchTopicPartitionRequest(tp.partition, offset, ctx.settings.fetchMaxBytes) }.toVector)
          }
          .toVector
    )

    doRequest(makeRequest, trace = false) {
      case KafkaResponse.Fetch(throttleTime, topics) =>
        F.pure {
          TopicPartitionResults.from {
            for {
              topic <- topics
              partition <- topic.partitions
            } yield {
              val topicPartition = TopicPartition(topic.topicName, partition.partition)
              val messages = partition.messages.map(x => RecordEntry(x.offset, Record(x.message.key, x.message.value))).toList

              (partition.kafkaResult, topicPartition, messages)
            }
          }
        }
    }
  }

  def metadata(topics: Set[String]) =
    doRequest(_ => KafkaRequest.Metadata(topics.toVector), trace = false) {
      case u: KafkaResponse.Metadata =>
        val brokers = u.brokers.map(broker => Broker(broker.nodeId, broker.host, broker.port))

        def toIor(x: MetadataTopicMetadataResponse): Ior[Set[TopicResult], Set[TopicPartitionResult[TopicInfo]]] = {
          if (x.kafkaResult == KafkaResult.NoError) {
            Ior.right(x.partitions.map(p => TopicPartitionResult(TopicPartition(x.topicName, p.id), TopicInfo(p.leader, p.replicas, p.isr))).toSet)
          } else {
            Ior.left(Set(TopicResult(x.topicName, x.kafkaResult)))
          }
        }

        F.pure(Metadata(brokers.toSet, Semigroup.combineAllOption(u.topicMetadata.map(toIor))))
    }

  def joinGroup(groupId: String, memberId: Option[String], protocol: String, protocols: Seq[GroupProtocol]) = {

    def makeGroupProtocolRequest(): F[List[JoinGroupProtocolRequest]] = {
      def encode(data: ConsumerProtocolMetadataData) = Async.fromAttempt(ConsumerProtocolMetadataData.codec.encode(data))
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
      def decode(bitVector: BitVector) = Async.fromAttempt(ConsumerProtocolMetadataData.codec.decodeValue(bitVector))
      members.toList.foldM(List.empty[GroupMember]) {
        case (acc, r) =>
          decode(BitVector(r.metadata)).map(data => acc :+ GroupMember(r.memberId, None, None, Some(extractConsumerProtocolMetadataData(data)), None))
      }
    }

    def extractResponse(u: KafkaResponse.JoinGroup): F[KafkaResult Xor JoinGroupResult] =
      if (u.kafkaResult === KafkaResult.NoError) {
        extractMembers(u.members).map(members => Xor.right(JoinGroupResult(u.generationId, u.groupProtocol, u.leaderId, u.memberId, members)))
      } else {
        F.pure(Xor.left(u.kafkaResult))
      }

    for {
      groupProtocolRequests <- KafkaReader.fromAsync[F, List[JoinGroupProtocolRequest]](ctx => makeGroupProtocolRequest())
      response <- doRequest(makeRequest(groupProtocolRequests.toVector), trace = true) {
        case u: KafkaResponse.JoinGroup => extractResponse(u)
      }
    } yield response
  }

  def produce(values: List[(TopicPartition, Record)]) = {
    def makeRequest(ctx: KafkaContext) = KafkaRequest.Produce(
      acks = 1,
      timeout = ctx.settings.produceTimeout.toMillis.toInt,
      topics = values
        .groupBy { case (tp, _) => tp.topic }
        .map {
          case (topic, tpvalues) =>
            ProduceTopicRequest(topic, tpvalues.groupBy(_._1.partition).map {
              case (partition, keyValues) =>
                val messages = keyValues.map {
                  case (_, message) =>
                    MessageSetEntry(offset = 0, message = Message(magicByte = 0, attributes = 0, key = message.key, value = message.value))
                }

                ProduceTopicPartitionRequest(partition, messages.toVector)
            }.toVector)
        }
        .toVector
    )

    doRequest(makeRequest, trace = false) {
      case KafkaResponse.Produce(topics, _) =>
        F.pure {
          TopicPartitionResults.from {
            for {
              topic <- topics
              partition <- topic.partitions
            } yield (partition.kafkaResult, TopicPartition(topic.topicName, partition.partition), partition.offset)
          }
        }
    }
  }

  def heartbeat(groupId: String, generationId: Int, memberId: String) =
    doRequest(_ => KafkaRequest.Heartbeat(groupId, generationId, memberId), trace = true) {
      case KafkaResponse.Heartbeat(kafkaResult) => F.pure(if (kafkaResult === KafkaResult.NoError) Xor.right(()) else Xor.left(kafkaResult))
    }

  def groupCoordinator(groupId: String) =
    doRequest(_ => KafkaRequest.GroupCoordinator(groupId), trace = false) {
      case u: KafkaResponse.GroupCoordinator =>
        F.pure(if (u.kafkaResult === KafkaResult.NoError) Xor.right(Broker(u.coordinatorId, u.coordinatorHost, u.coordinatorPort)) else Xor.left(u.kafkaResult))
    }

  def pure[A](x: A) = KafkaReader.pure(x)
  def flatMap[A, B](fa: KafkaReader[F, A])(f: (A) => KafkaReader[F, B]) = fa.flatMap(f)
  def tailRecM[A, B](a: A)(f: (A) => KafkaReader[F, Either[A, B]]): KafkaReader[F, B] = flatMap(f(a)) {
    case Left(ohh)  => tailRecM(ohh)(f)
    case Right(ohh) => pure(ohh)
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
  }
}
