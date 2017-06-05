package flumina.akkaimpl

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern._
import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

final class MapVar[K, V](initial: Map[K, V], update: (Map[K, Cell[V]], K, Cell[V]) => Map[K, Cell[V]])(implicit S: ActorSystem, T: Timeout) {

  private final class Handler extends Actor {

    import context.dispatcher

    private def currentMap(current: Map[K, Option[V]]) = current.foldLeft(Map.empty[K, V]) {
      case (acc, (k, Some(v))) => acc.updated(k, v)
      case (acc, (k, None))    => acc
    }

    private def nonExistingKeys(subscription: List[Subscription], keys: Set[K]): Set[K] =
      keys -- subscription.flatMap(x => x.update.keySet).toSet

    private def updateSubset(subscriptions: List[Subscription], subset: Map[K, Cell[V]]): List[Subscription] =
      subscriptions.map(x => x.updateSubset(subset))

    private case class Subscription(update: Map[K, Cell[V]], subscriber: ActorRef) {
      def updateSubset(subset: Map[K, Cell[V]]): Subscription =
        copy(update = update ++ subset.filterKeys(update.keySet))

      def isComplete: Boolean = update.values.forall {
        case Cell.Success(_)           => true
        case Cell.Failed(_)            => true
        case Cell.Empty | Cell.Pending => false
      }

      def subset: Map[K, V] = update.flatMap { case (k, v) => Cell.toOption(v).map(k -> _).toList }
    }

    private def active(current: Map[K, Option[V]], subscriptions: List[Subscription]): Receive = {

      case UpdateSubset(keys, updater) =>

        val nonExisting = nonExistingKeys(subscriptions, keys)
        val existing = keys -- nonExisting
        val map = nonExisting.map(k => k -> (Cell.Pending: Cell[V])) ++ existing.map(k => k -> (Cell.Empty: Cell[V]))

        if (nonExisting.nonEmpty) {
          updater(nonExisting)
            .map(kvs => UpdatedSubset(kvs.map { case (k, v) => k -> Cell.Success(v) }))
            .recoverWith { case NonFatal(ex) => Future.successful(UpdatedSubset(keys.map(_ -> Cell.Failed(ex)).toSeq)) } pipeTo self
        }

        val newCurrent = nonExisting.foldLeft(current) { case (acc, k) => acc.updated(k, None) }

        context.become(active(newCurrent, subscriptions :+ Subscription(map.toMap, sender())))

      case GetOrElseBatch(keys, orElse) =>
        val nonExisting = nonExistingKeys(subscriptions, keys)

        if (nonExisting.nonEmpty) {
          orElse(nonExisting)
            .map(kvs => UpdatedSubset(kvs.map { case (k, v) => k -> Cell.Success(v) }))
            .recoverWith { case NonFatal(ex) => Future.successful(UpdatedSubset(keys.map(_ -> Cell.Failed(ex)).toSeq)) } pipeTo self

          val existing = keys -- nonExisting
          val existingMap = existing.flatMap(k => current.get(k).toList.map(v => k -> v.fold[Cell[V]](Cell.Pending)(_ => Cell.Empty))).toMap
          val nonExistingMap = nonExisting.map(_ -> (Cell.Pending: Cell[V])).toMap
          val map = existingMap ++ nonExistingMap
          val newCurrent = nonExisting.foldLeft(current) { case (acc, k) => acc.updated(k, None) }

          context.become(active(newCurrent, subscriptions :+ Subscription(map, sender())))
        } else {
          sender() ! currentMap(current.filterKeys(keys))
        }

      case GetOrElseSingle(key, orElse) => current.get(key) match {
        case Some(v) => v.foreach(value => sender() ! Map(key -> value))
        case None =>
          val nk = nonExistingKeys(subscriptions, Set(key))
          if (nk.nonEmpty) {
            orElse(key)
              .map(v => UpdatedSubset(Seq(key -> Cell.Success(v))))
              .recoverWith { case NonFatal(ex) => Future.successful(UpdatedSubset(Seq(key -> Cell.Failed(ex)))) } pipeTo self
          }
          context.become(active(current, subscriptions :+ Subscription(Map(key -> Cell.Pending), sender())))
      }

      case UpdatedSubset(updates) =>

        val currentMap = current.mapValues(_.fold[Cell[V]](Cell.Empty)(x => Cell.Success(Some(x))))
        val newCellMap = updates.foldLeft(currentMap) {
          case (acc, (key, value)) =>
            update(acc, key, value)
        }
        val newSubscriptions = updateSubset(subscriptions, newCellMap)

        newSubscriptions.filter(_.isComplete).foreach(x => x.subscriber ! x.subset)

        val prunedSubscriptions = newSubscriptions.filterNot(_.isComplete)
        val newMap = newCellMap.mapValues(Cell.toOption)

        context.become(active(newMap, prunedSubscriptions))

      case Get => sender() ! currentMap(current)
    }

    def receive: Receive = active(initial.mapValues(Option.apply), List.empty)
  }

  private case class UpdateSubset(keys: Set[K], updater: Set[K] => Future[List[(K, Option[V])]])
  private case class GetOrElseSingle(key: K, orElse: K => Future[Option[V]])
  private case class GetOrElseBatch(keys: Set[K], orElse: Set[K] => Future[List[(K, Option[V])]])
  private case class UpdatedSubset(updates: Seq[(K, Cell[V])])
  private case object Get

  private val handler = S.actorOf(Props(new Handler))

  def getOrElseUpdate(key: K, orElse: K => Future[Option[V]])(implicit EC: ExecutionContext): Future[Option[V]] =
    handler.ask(GetOrElseSingle(key, orElse)).mapTo[Map[K, V]].map(_.get(key))

  def getSubsetOrElseUpdate(keys: Set[K], orElse: Set[K] => Future[List[(K, Option[V])]]): Future[Map[K, V]] =
    handler.ask(GetOrElseBatch(keys, orElse)).mapTo[Map[K, V]]

  def updateSubset(keys: Set[K], updater: Set[K] => Future[List[(K, Option[V])]]): Future[Map[K, V]] =
    handler.ask(UpdateSubset(keys, updater)).mapTo[Map[K, V]]

  def get: Future[Map[K, V]] = handler.ask(Get).mapTo[Map[K, V]]
}

object MapVar {
  def apply[K, V](initial: Map[K, V])(update: (Map[K, Cell[V]], K, Cell[V]) => Map[K, Cell[V]])(implicit S: ActorSystem, T: Timeout, EC: ExecutionContext): MapVar[K, V] =
    new MapVar(initial, update)

  object updates {
    def appendAndOmit[K, V](current: Map[K, Cell[V]], key: K, value: Cell[V]): Map[K, Cell[V]] = value match {
      case Cell.Success(v)           => current.updated(key, Cell.Success(v))
      case Cell.Failed(ex)           => current.updated(key, Cell.Failed(ex))
      case Cell.Pending | Cell.Empty => current
    }
  }
}

final class UVar[A: ClassTag] private (initial: A)(implicit S: ActorSystem, T: Timeout) {

  private final class Handler extends Actor {
    import context.dispatcher

    private def updating(listeners: Seq[ActorRef]): Receive = {
      case Get       => context.become(updating(listeners :+ sender()))
      case Update(_) => context.become(updating(listeners :+ sender()))
      case value: A =>
        listeners.foreach(_ ! value)
        context.become(active(value))
    }

    private def active(value: A): Receive = {
      case Get => sender() ! value
      case Update(update) =>
        update(value) pipeTo self
        context.become(updating(Seq(sender())))
    }

    override def receive: Receive = active(initial)
  }

  private case object Get
  private sealed case class Update(update: A => Future[A])

  private val actorRef = S.actorOf(Props(new Handler))

  def get: Future[A] = actorRef.ask(Get).mapTo[A]
  def update(f: A => Future[A]): Future[A] = actorRef.ask(Update(f)).mapTo[A]
}

object UVar {
  def apply[A: ClassTag](initial: A)(implicit S: ActorSystem, T: Timeout): UVar[A] = new UVar(initial)
}

sealed trait Cell[+A]

object Cell {
  case object Empty extends Cell[Nothing]
  case object Pending extends Cell[Nothing]
  final case class Failed(err: Throwable) extends Cell[Nothing]
  final case class Success[A](value: Option[A]) extends Cell[A]

  def toOption[A](cell: Cell[A]): Option[A] = cell match {
    case Empty | Pending => None
    case Failed(_)       => None
    case Success(v)      => v
  }
}
