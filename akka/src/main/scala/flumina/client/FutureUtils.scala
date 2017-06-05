package flumina.client

import java.util.{Timer, TimerTask}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object FutureUtils {

  private val timer = new Timer()

  def delay(duration: FiniteDuration): Future[Unit] = {
    val promise = Promise[Unit]()
    val task = new TimerTask {
      def run() = promise.complete(Try(()))
    }
    timer.schedule(task, duration.toMillis)
    promise.future
  }

  def delayFuture[A](duration: FiniteDuration, f: => Future[A])(implicit EC: ExecutionContext): Future[A] =
    delay(duration).flatMap(_ => f)

}
