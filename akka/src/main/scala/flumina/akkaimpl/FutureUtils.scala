package flumina.akkaimpl

import java.util.{Timer, TimerTask}

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

object FutureUtils {

  private val timer = new Timer()

  def delay(duration: FiniteDuration) = {
    val promise = Promise[Unit]()
    val task = new TimerTask {
      def run() = promise.complete(Try(()))
    }
    timer.schedule(task, duration.toMillis)
    promise.future
  }

}
