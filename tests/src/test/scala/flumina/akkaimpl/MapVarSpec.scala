package flumina.akkaimpl

import akka.actor.ActorSystem
import akka.util.Timeout
import cats.implicits._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, Suite, WordSpecLike }

import scala.concurrent.Future
import scala.concurrent.duration._

class MapVarSpec extends Suite with WordSpecLike with ScalaFutures with Matchers {

  implicit val system = ActorSystem("default")
  implicit val to = Timeout(2.seconds)
  implicit val ec = system.dispatcher

  def newMap = MapVar(Map.empty[String, String])(MapVar.updates.appendAndOmit)
  def filledMap = MapVar(Map("c" -> "3", "d" -> "4"))(MapVar.updates.appendAndOmit)

  "MapVar" should {
    "construct" in {
      newMap
    }

    "get: simple" in {
      whenReady(filledMap.get)(_ shouldBe Map("c" -> "3", "d" -> "4"))
    }

    "getOrElseUpdate: simple" in {
      def update(key: String) = Future.successful(Some("test"))

      whenReady(filledMap.getOrElseUpdate("c", update))(_ shouldBe Some("3"))
    }

    "getSubsetOrElseUpdate: update" in {
      def update(keys: Set[String]): Future[List[(String, Option[String])]] =
        Future.successful(List("a" -> Option("1"), "b" -> Option("2")))

      whenReady(filledMap.getSubsetOrElseUpdate(Set("a", "b"), update))(_ shouldBe Map("a" -> "1", "b" -> "2"))
    }

    "updateSubset: simple" in {
      def update(keys: Set[String]): Future[List[(String, Option[String])]] =
        Future.successful(List("c" -> Option("1"), "d" -> Option("2")))

      whenReady(filledMap.updateSubset(Set("c", "d"), update))(_ shouldBe Map("c" -> "1", "d" -> "2"))
    }

    "updateSubset: concurrent" in {
      def update1(keys: Set[String]): Future[List[(String, Option[String])]] =
        Future.successful(List("a" -> Option("1"), "b" -> Option("2")))

      def update2(keys: Set[String]): Future[List[(String, Option[String])]] =
        Future.successful(List("c" -> Option("1"), "d" -> Option("2")))

      whenReady(filledMap.updateSubset(Set("a", "b"), update1) zip filledMap.updateSubset(Set("c", "d"), update2)) {
        case (x, y) =>
          x shouldBe Map("a" -> "1", "b" -> "2")
          y shouldBe Map("c" -> "1", "d" -> "2")
      }
    }

    "updateSubset: first update should block second" in {
      def update1(keys: Set[String]): Future[List[(String, Option[String])]] =
        FutureUtils.delay(100.milliseconds).flatMap(_ => Future.successful(List("a" -> Option("1"), "b" -> Option("2"), "c" -> Option("3"))))

      def update2(keys: Set[String]): Future[List[(String, Option[String])]] =
        Future.successful(List("a" -> Option("3"), "b" -> Option("4")))

      val map = filledMap

      whenReady((map.updateSubset(Set("a", "b", "c"), update1) |@| map.updateSubset(Set("a", "b"), update2)).tupled) {
        case (x, y) =>
          x shouldBe Map("a" -> "1", "b" -> "2", "c" -> "3")
          y shouldBe Map("a" -> "1", "b" -> "2")
      }
    }

    "updateSubset: failing future" in {
      def update1(keys: Set[String]): Future[List[(String, Option[String])]] =
        FutureUtils.delay(100.milliseconds).flatMap(_ => Future.failed(new Exception("Oh noes")))

      def update2(keys: Set[String]): Future[List[(String, Option[String])]] =
        Future.successful(List("a" -> Option("3"), "b" -> Option("4")))

      val map = filledMap

      whenReady((map.updateSubset(Set("c"), update1) |@| map.updateSubset(Set("a", "b", "c"), update2)).tupled) {
        case (x, y) =>
          x shouldBe Map()
          y shouldBe Map("a" -> "3", "b" -> "4")
      }
    }

  }

  override implicit def patienceConfig = PatienceConfig(timeout = 60.seconds, interval = 10.milliseconds)

}
