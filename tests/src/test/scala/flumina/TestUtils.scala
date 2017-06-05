package flumina

import org.scalatest.matchers.{MatchResult, Matcher}

import scala.annotation.tailrec

object TestUtils {

  private class RespectOrderMatcher[A](f: A => Long) extends Matcher[Seq[A]] {
    override def apply(left: Seq[A]): MatchResult = {
      @tailrec
      def loop(list: List[A]): MatchResult = list match {
        case first :: second :: tail =>
          if (f(first) < f(second)) loop(second :: tail)
          else MatchResult(false, s"$first is not less $second", s"$first is less $second")
        case _ => MatchResult(matches = true, "", "")
      }
      loop(left.toList)
    }
  }

  def respectOrder[A](f: A => Long): Matcher[Seq[A]] = new RespectOrderMatcher[A](f)
}
