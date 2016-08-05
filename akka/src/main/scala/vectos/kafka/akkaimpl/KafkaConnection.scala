package vectos.kafka.akkaimpl

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Status}
import akka.io._
import akka.util.{ByteIterator, ByteString}
import scodec.Attempt
import vectos.kafka.types.{RequestEnvelope, ResponseEnvelope}

import scala.annotation.tailrec

/**
 * A connection to kafka, low level IO TCP akka implementation.
 * Handles folding and unfolding of envelopes, reconnection and encoding/decoding message size and buffering of incoming
 * messages.
 *
 * We do not back pressure on the OS' TCP buffers
 *
 * TODO: We should evaluate if this will become a problem in a highly concurrent environment with large messages.
 */
final class KafkaConnection private (pool: ActorRef, manager: ActorRef, broker: KafkaBroker.Node, retryStrategy: KafkaConnectionRetryStrategy) extends Actor with ActorLogging {

  import Tcp._
  import context.dispatcher

  private final case class Receiver(address: ActorRef, trace: Boolean)

  private final case class ConnectedState(connection: ActorRef, inFlightRequests: Map[Int, Receiver], nextCorrelationId: Int) {
    def toBuffering(buffer: ByteString, bytesToRead: Int) = BufferingState(buffer, bytesToRead, connection, inFlightRequests, nextCorrelationId)
  }
  private final case class BufferingState(buffer: ByteString, bytesToRead: Int, connection: ActorRef, inFlightRequests: Map[Int, Receiver], nextCorrelationId: Int) {
    def toConnectedWithoutCorrelationId(withoutCorrelationId: Int) = ConnectedState(connection, inFlightRequests - withoutCorrelationId, nextCorrelationId)
    def toConnected = ConnectedState(connection, inFlightRequests, nextCorrelationId)
  }

  override def preStart() = connect()

  def reconnect(timesTriedToConnect: Int) = {
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

  def connect() = {
    log.info(s"Connecting to $broker")
    manager ! Tcp.Connect(new InetSocketAddress("localhost", broker.port))
  }

  def connecting(timesTriedToConnect: Int): Actor.Receive = {
    case CommandFailed(_) =>
      context.become(connecting(timesTriedToConnect + 1))
      reconnect(timesTriedToConnect + 1)

    case Connected(remote, local) =>
      val connection = sender()
      pool ! KafkaConnectionPool.BrokerUp(self, broker)
      connection ! Register(self, keepOpenOnPeerClosed = true)
      context.become(connected(ConnectedState(connection, Map(), Int.MinValue)))
  }

  def connected(state: ConnectedState): Actor.Receive = {
    case request: KafkaConnectionRequest =>
      encodeEnvelope(state.nextCorrelationId, request) match {
        case Attempt.Successful(msg) =>
          state.connection ! Write(msg)
          context.become(
            connected(
              state.copy(
                inFlightRequests = state.inFlightRequests + (state.nextCorrelationId -> Receiver(sender(), request.trace)),
                nextCorrelationId = state.nextCorrelationId + 1 //overflowing is oke
              )
            )
          )
        case Attempt.Failure(err) => log.warning(s"Error encoding: ${err.messageWithContext}")
      }

    case CommandFailed(w: Write) =>
      log.warning(s"Failed to write... $w")

    case Received(data) =>
      if (data.size >= 4) {
        val readSize = bigEndianDecoder(data.take(4).iterator, 4)
        val buffer = data.drop(4)

        if (buffer.size === readSize) {
          decodeEnvelope(buffer) match {
            case Attempt.Successful(env) =>
              state.inFlightRequests.get(env.correlationId) match {
                case Some(receiver) =>
                  if (receiver.trace) {
                    log.debug(s"Response for ${env.correlationId} -> ${env.response.toHex}")
                  }
                  receiver.address ! Status.Success(env.response)
                case None => log.warning(s"There was no receiver for ${env.correlationId}")
              }
              context.become(connected(state.copy(inFlightRequests = state.inFlightRequests - env.correlationId)))

            case Attempt.Failure(err) =>
              log.warning(s"Error decoding: ${err.messageWithContext}")
          }
        } else {
          context.become(buffering(state.toBuffering(buffer, readSize)))
        }

      } else {
        log.warning("Received data was smaller then 4? Should not go wrong...")
      }
    case _: ConnectionClosed =>
      pool ! KafkaConnectionPool.BrokerDown(self, broker)
      context.become(connecting(0))
      reconnect(0)
  }

  def buffering(state: BufferingState): Actor.Receive = {
    case request: KafkaConnectionRequest =>
      encodeEnvelope(correlationId = state.nextCorrelationId, request = request) match {
        case Attempt.Successful(msg) =>
          state.connection ! Write(msg)
          context.become(
            buffering(
              state.copy(
                inFlightRequests = state.inFlightRequests + (state.nextCorrelationId -> Receiver(sender(), request.trace)),
                nextCorrelationId = state.nextCorrelationId + 1 //overflowing is oke
              )
            )
          )
        case Attempt.Failure(err) =>
          log.warning(s"Error writing: ${err.messageWithContext}")
      }

    case CommandFailed(w: Write) => log.warning(s"Failed to write... $w")

    case Received(data) =>
      val newBuffer = state.buffer ++ data
      if (newBuffer.size === state.bytesToRead) {
        decodeEnvelope(newBuffer) match {
          case Attempt.Successful(env) =>
            state.inFlightRequests.get(env.correlationId) match {
              case Some(receiver) =>
                if (receiver.trace) {
                  log.debug(s"Response for ${env.correlationId} -> ${env.response.toHex}")
                }
                receiver.address ! Status.Success(env.response)
              case None => log.warning(s"There was no receiver for ${env.correlationId}")
            }
            context.become(connected(state.toConnectedWithoutCorrelationId(env.correlationId)))

          case Attempt.Failure(err) =>
            log.warning(s"Error encoding: ${err.messageWithContext}")
            context.become(connected(state.toConnected))
        }
      } else {
        context.become(buffering(state.copy(buffer = newBuffer)))
      }

    case _: ConnectionClosed =>
      pool ! KafkaConnectionPool.BrokerDown(self, broker)
      context.become(connecting(0))
      reconnect(0)
  }

  def receive = connecting(timesTriedToConnect = 0)

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

  def encodeEnvelope(correlationId: Int, request: KafkaConnectionRequest) = {
    RequestEnvelope.codec.encode(RequestEnvelope(request.apiKey, request.version, correlationId, Some("scala-kafka"), request.requestPayload)).map { bytes =>

      val msg = bytes.toByteVector.toByteString
      val msgSize = msg.size
      val header = ByteString((msgSize >> 24) & 0xFF, (msgSize >> 16) & 0xFF, (msgSize >> 8) & 0xFF, msgSize & 0xFF)

      if (request.trace) {
        log.debug(s"Request $correlationId encoded ($msgSize) -> ${bytes.toHex}")
      }

      header ++ msg
    }
  }

  def decodeEnvelope(byteString: ByteString) = ResponseEnvelope.codec.decodeValue(byteString.toByteVector.toBitVector)
}

object KafkaConnection {
  def props(pool: ActorRef, manager: ActorRef, broker: KafkaBroker.Node, retryStrategy: KafkaConnectionRetryStrategy) = Props(new KafkaConnection(pool, manager, broker, retryStrategy))
}
