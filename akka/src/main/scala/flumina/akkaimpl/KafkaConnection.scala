package flumina.akkaimpl

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.io._
import akka.util.{ByteIterator, ByteString}
import scodec.Attempt
import flumina.core.{RequestEnvelope, ResponseEnvelope}
import flumina.core.ir._

import scala.annotation.tailrec

/**
 * A connection to kafka, low level IO TCP akka implementation.
 * Handles folding and unfolding of envelopes, reconnection and encoding/decoding message size and buffering of incoming
 * messages.
 */
final class KafkaConnection private (pool: ActorRef, manager: ActorRef, broker: KafkaBroker.Node, retryStrategy: KafkaConnectionRetryStrategy) extends Actor with ActorLogging {

  import Tcp._
  import context.dispatcher

  private case class Receiver(address: ActorRef, request: KafkaConnectionRequest, startTime: Long)
  private sealed case class Ack(offset: Int) extends Tcp.Event

  private val maxStored = 100000000L

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var readBuffer: ByteString = ByteString.empty
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var bytesToRead: Option[Int] = None
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var connection: ActorRef = _
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var inFlightRequests: Map[Int, Receiver] = Map()
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var nextCorrelationId: Int = 0 //Int.MinValue
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var writeBuffer: Vector[ByteString] = Vector()
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var storageOffset: Int = 0
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var stored: Int = 0

  private def currentOffset = storageOffset + writeBuffer.size

  override def preStart(): Unit = connect()

  private def reconnect(timesTriedToConnect: Int) = {
    retryStrategy.nextDelay(timesTriedToConnect) match {
      case Some(duration) =>
        log.info(s"Will retry to reconnect in: $duration")
        context.system.scheduler.scheduleOnce(duration, new Runnable {
          override def run(): Unit = connect()
        })

      case None =>
        log.error(s"Tried to reconnect $timesTriedToConnect a few times, but failed! shutting down")
        context stop self
    }
  }

  private def connect() = {
    log.debug(s"Connecting to $broker")
    manager ! Tcp.Connect(new InetSocketAddress("localhost", broker.port))
  }

  private def connecting(timesTriedToConnect: Int): Actor.Receive = {
    case CommandFailed(_) =>
      context.become(connecting(timesTriedToConnect + 1))
      reconnect(timesTriedToConnect + 1)

    case Connected(remote, local) =>
      connection = sender()
      pool ! KafkaConnectionPool.BrokerUp(self, broker)
      connection ! Register(self, keepOpenOnPeerClosed = true)
      context.become(connected)
  }

  private def buffer(data: ByteString): Unit = {
    writeBuffer :+= data
    stored += data.size

    if (stored > maxStored) {
      log.error(s"drop connection due buffer overrun")
      context stop self
    }
  }

  private def acknowledge(ack: Int) = {
    require(ack === storageOffset, s"received ack $ack at $storageOffset")
    require(writeBuffer.nonEmpty, s"writeBuffer was empty at ack $ack")

    val size = writeBuffer(0).size
    stored -= size

    storageOffset += 1
    writeBuffer = writeBuffer drop 1
  }

  private def read(data: ByteString) = {

    def decode(buffer: ByteString) = {
      decodeEnvelope(buffer) match {
        case Attempt.Successful(env) =>
          inFlightRequests.get(env.correlationId) match {
            case Some(receiver) =>
              if (receiver.request.trace) {
                log.info(s"<- [corrId: ${env.correlationId}] ${env.response.toHex}")
              }

              val timeTaken = System.currentTimeMillis() - receiver.startTime

              if (timeTaken > 200) {
                log.warning(s"Request [api-key: ${receiver.request.apiKey}, size: ${receiver.request.requestPayload.size}] took $timeTaken ms to execute")
              }

              receiver.address ! Status.Success(env.response)
            case None => log.warning(s"There was no receiver for ${env.correlationId}")
          }

          inFlightRequests -= env.correlationId

        case Attempt.Failure(err) =>
          log.error(s"Error decoding: ${err.messageWithContext}")
      }
    }

    def run(toRead: Option[Int], currentBuffer: ByteString): (Option[Int], ByteString) = toRead match {
      case Some(r) =>
        if (currentBuffer.size >= r) {
          decode(currentBuffer.take(r))
          run(None, currentBuffer.drop(r))
        } else {
          toRead -> currentBuffer
        }
      case None =>
        if (currentBuffer.size >= 4) {
          run(Some(bigEndianDecoder(currentBuffer.take(4).iterator, 4)), currentBuffer.drop(4))
        } else {
          None -> currentBuffer
        }
    }

    val (read, buffer) = run(bytesToRead, readBuffer ++ data)

    bytesToRead = read
    readBuffer = buffer
  }

  private def writeFirst(): Unit = {
    connection ! Write(writeBuffer(0), Ack(storageOffset))
  }

  private def writeAll(): Unit = {
    for ((data, i) <- writeBuffer.zipWithIndex) {
      connection ! Write(data, Ack(storageOffset + i))
    }
  }

  private def goReconnect() = {
    //TODO: reset all state?
    pool ! KafkaConnectionPool.BrokerDown(self, broker)
    context.become(connecting(0))
    reconnect(0)
  }

  private def connected: Actor.Receive = {
    case request: KafkaConnectionRequest =>
      encodeEnvelope(nextCorrelationId, request) match {
        case Attempt.Successful(msg) =>
          connection ! Write(msg, Ack(currentOffset))
          buffer(msg)
          inFlightRequests += (nextCorrelationId -> Receiver(sender(), request, System.currentTimeMillis()))
          if (request.trace) {
            log.info(s"-> [apiKey: ${request.apiKey}, corrId: $nextCorrelationId] ${request.requestPayload.toHex}")
          }
          nextCorrelationId += 1

        case Attempt.Failure(err) => log.error(s"Error encoding: ${err.messageWithContext}")
      }
    case Ack(ack) => acknowledge(ack)
    case CommandFailed(Write(_, Ack(ack))) =>
      connection ! ResumeWriting
      context become buffering(nack = ack, toAck = 10, peerClosed = false)

    case Received(data) => read(data)
    case PeerClosed =>
      if (writeBuffer.isEmpty) context stop self
      else context become closing

    case Aborted => goReconnect()
    case Closed  => goReconnect()

  }

  private def buffering(nack: Int, toAck: Int, peerClosed: Boolean): Actor.Receive = {
    case request: KafkaConnectionRequest =>
      encodeEnvelope(nextCorrelationId, request) match {
        case Attempt.Successful(msg) =>
          buffer(msg)
          inFlightRequests += (nextCorrelationId -> Receiver(sender(), request, System.currentTimeMillis()))
          if (request.trace) {
            log.info(s"-> [apiKey: ${request.apiKey}, corrId: $nextCorrelationId] ${request.requestPayload.toHex}")
          }
          nextCorrelationId += 1

        case Attempt.Failure(err) => log.error(s"Error encoding: ${err.messageWithContext}")
      }

    case WritingResumed         => writeFirst()
    case Received(data)         => read(data)
    case PeerClosed             => context.become(buffering(nack, toAck, peerClosed = true))
    case Aborted                => goReconnect()
    case Closed                 => goReconnect()

    case Ack(ack) if ack < nack => acknowledge(ack)
    case Ack(ack) =>
      acknowledge(ack)
      if (writeBuffer.nonEmpty) {
        if (toAck > 0) {
          // stay in ACK-based mode for a while
          writeFirst()
          context.become(buffering(nack, toAck - 1, peerClosed))
        } else {
          // then return to NACK-based again
          writeAll()
          context become (if (peerClosed) closing else connected)
        }
      } else if (peerClosed) context stop self
      else context become connected
  }

  private def closing: Actor.Receive = {
    case CommandFailed(_: Write) =>
      connection ! ResumeWriting
      context.become({

        case WritingResumed =>
          writeAll()
          context.unbecome()

        case ack: Int => acknowledge(ack)

      }, discardOld = false)

    case Ack(ack) =>
      acknowledge(ack)
      if (writeBuffer.isEmpty) context stop self
  }

  def receive: Receive = connecting(timesTriedToConnect = 0)

  private val bigEndianDecoder: (ByteIterator, Int) ⇒ Int = (bs, length) ⇒ {
    @tailrec
    def run(count: Int, decoded: Int): Int = {
      if (count > 0) {
        run(count - 1, decoded << 8 | bs.next().toInt & 0xFF)
      } else {
        decoded
      }
    }

    run(length, 0)
  }

  private def encodeEnvelope(correlationId: Int, request: KafkaConnectionRequest) = {
    RequestEnvelope.codec.encode(RequestEnvelope(request.apiKey, request.version, correlationId, Some("flumina"), request.requestPayload)).map { bytes =>

      val msg = bytes.toByteString
      val msgSize = msg.size
      val header = ByteString((msgSize >> 24) & 0xFF, (msgSize >> 16) & 0xFF, (msgSize >> 8) & 0xFF, msgSize & 0xFF)

      header ++ msg
    }
  }

  private def decodeEnvelope(byteString: ByteString) = ResponseEnvelope.codec.decodeValue(byteString.toBitVector)
}

object KafkaConnection {
  def props(pool: ActorRef, manager: ActorRef, broker: KafkaBroker.Node, retryStrategy: KafkaConnectionRetryStrategy): Props =
    Props(new KafkaConnection(pool, manager, broker, retryStrategy))
}
