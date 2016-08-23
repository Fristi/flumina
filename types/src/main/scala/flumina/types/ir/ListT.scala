package flumina.types.ir

import cats._
import cats.functor.Bifunctor
import cats.syntax.all._

final case class ListT[F[_], A](stepListT: F[TStep[A, ListT[F, A]]]) {
  def ++(other: ListT[F, A])(implicit F: Monad[F]): ListT[F, A] = ListT(for {
    s <- stepListT
    r <- s match {
      case TNil() =>
        other.stepListT
      case TCons(a, x) =>
        TStep.cons(a, x ++ other).pure[F]
    }
  } yield r)

  def map[B](f: A => B)(implicit F: Monad[F]): ListT[F, B] =
    ListT(stepListT.map(x => x.bimap(f, _.map(f))))

  def flatMap[B](f: A => ListT[F, B])(implicit F: Monad[F]): ListT[F, B] = ListT(for {
    s <- stepListT
    r <- s match {
      case TNil() =>
        TStep.nil[B, ListT[F, B]].pure[F]
      case TCons(a, x) =>
        (f(a) ++ x.flatMap(f)).stepListT
    }
  } yield r)

  def run(implicit F: Monad[F]): F[List[A]] = for {
    s <- stepListT
    r <- s match {
      case TNil()      => Nil.pure[F]
      case TCons(a, x) => x.run.map(a :: _)
    }
  } yield r
}

object ListT {
  def singleton[F[_]: Monad, A](a: A): ListT[F, A] =
    cons(a, nil[F, A])

  def nil[F[_]: Monad, A]: ListT[F, A] =
    ListT(TStep.nil[A, ListT[F, A]].pure[F])

  def cons[F[_]: Monad, A](a: A, as: ListT[F, A]): ListT[F, A] =
    ListT(TStep.cons[A, ListT[F, A]](a, as).pure[F])

  def hoist[F[_]: Monad, A](xs: List[A]): ListT[F, A] =
    xs.foldRight(nil[F, A])((el, acc) => cons(el, acc))

  def fromOption[F[_]: Monad, A](option: Option[A], orElse: => F[A]): ListT[F, A] = option match {
    case Some(value) => ListT.singleton(value)
    case None        => ListT.lift(orElse)
  }

  def filter[F[_]: Monad, A](ls: List[A])(f: A => F[A]): ListT[F, A] = ls match {
    case x :: xs => ListT.lift[F, A](f(x)) ++ filter(xs)(f)
    case Nil     => ListT.nil[F, A]
  }

  def lift[F[_]: Monad, A](f: F[A]): ListT[F, A] =
    ListT(f.map(x => TStep.cons(x, nil[F, A])))

  implicit def ListTInstances[F[+_]: Monad]: Monad[ListT[F, ?]] with MonoidK[ListT[F, ?]] = new Monad[ListT[F, ?]] with MonoidK[ListT[F, ?]] {
    override def pure[A](x: A): ListT[F, A] = singleton(x)
    override def flatMap[A, B](fa: ListT[F, A])(f: (A) => ListT[F, B]): ListT[F, B] = fa.flatMap(f)
    override def empty[A]: ListT[F, A] = nil[F, A]
    override def combineK[A](x: ListT[F, A], y: ListT[F, A]): ListT[F, A] = x ++ y
  }
}

sealed trait TStep[A, X] {
  def bimap[B, Y](f: A => B, g: X => Y): TStep[B, Y] =
    this match {
      case TNil()      => TNil()
      case TCons(a, x) => TCons(f(a), g(x))
    }
}

final case class TNil[A, X]() extends TStep[A, X]
final case class TCons[A, X](a: A, x: X) extends TStep[A, X]

object TStep {
  def nil[A, X]: TStep[A, X] =
    TNil[A, X]()

  def cons[A, X](a: A, x: X): TStep[A, X] =
    TCons(a, x)

  implicit def TStepBifunctor: Bifunctor[TStep] = new Bifunctor[TStep] {
    def bimap[A, B, C, D](ab: TStep[A, B])(f: A => C, g: B => D) =
      ab.bimap(f, g)
  }
}