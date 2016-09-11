package flumina

import akka.util.ByteString
import flumina.core.Async
import scodec.bits.BitVector

import scala.concurrent.{ExecutionContext, Future}

package object akkaimpl {

  implicit class EnrichedByteString(val value: ByteString) extends AnyVal {
    def toBitVector: BitVector = BitVector.view(value.asByteBuffer)
  }

  implicit class EnrichedBitVector(val value: BitVector) extends AnyVal {
    def toByteString: ByteString = ByteString(value.toByteBuffer)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  implicit final class AnyOps[A](self: A) {
    @inline
    def ===(other: A): Boolean = self == other
  }

  implicit def futureAsyncInstance(implicit ec: ExecutionContext): Async[Future] = new Async[Future] {

    def fail[A](err: Throwable): Future[A] = Future.failed(err)

    def pure[A](x: A): Future[A] = Future.successful(x)

    def flatMap[A, B](fa: Future[A])(f: (A) => Future[B]): Future[B] = fa.flatMap(f)

    def tailRecM[A, B](a: A)(f: (A) => Future[Either[A, B]]): Future[B] = f(a).flatMap {
      case Left(b1) => tailRecM(b1)(f)
      case Right(c) => Future.successful(c)
    }
  }

}

