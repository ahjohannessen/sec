package sec
package core

import cats.{Eq, Order, Show}
import cats.implicits._

//======================================================================================================================

sealed trait StreamRevision
object StreamRevision {

  case object NoStream     extends StreamRevision
  case object Any          extends StreamRevision
  case object StreamExists extends StreamRevision

  implicit val eq: Eq[StreamRevision] = Eq.fromUniversalEquals
  implicit val showForStreamRevision: Show[StreamRevision] = Show.show {
    case NoStream             => "NoStream"
    case Any                  => "Any"
    case StreamExists         => "StreamExists"
    case EventNumber.Exact(v) => s"Exact($v)"
  }

}

//======================================================================================================================

sealed trait EventNumber
object EventNumber {

  val Start: Exact = Exact(0L)

  sealed abstract case class Exact(value: Long) extends EventNumber with StreamRevision
  object Exact {
    private[sec] def apply(value: Long): Exact = new Exact(value) {}
    def opt(value: Long): Option[Exact]        = if (value >= 0) apply(value).some else none

  }

  private[sec] def unapply(en: EventNumber): Option[Long] = en match {
    case Exact(v) => v.some
    case End      => End.longValue.some
  }

  case object End extends EventNumber {
    private[sec] final val longValue: Long = -1
  }

  ///

  def apply(number: Long): EventNumber = if (number < 0) End else Exact(number)

  implicit val orderForEventNumber: Order[EventNumber] = Order.from {
    case (x: Exact, y: Exact) => x.value compare y.value
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

  implicit val showForExact: Show[Exact] = Show.show[Exact] {
    case Exact(v) => s"EventNumber($v)"
  }

  implicit val showForEventNumber: Show[EventNumber] = Show.show[EventNumber] {
    case e: Exact => e.show
    case End      => "end"
  }
}

//======================================================================================================================

sealed trait Position
object Position {

  val Start: Exact = Exact(0L, 0L)

  sealed abstract case class Exact(commit: Long, prepare: Long) extends Position
  object Exact {
    private[sec] def apply(commit: Long, prepare: Long): Exact = new Exact(commit, prepare) {}
  }

  case object End extends Position {
    private[sec] val longValues: (Long, Long) = (-1L, -1L)
  }

  def unapply(pos: Position): Option[(Long, Long)] = pos match {
    case Exact(p, c) => (p, c).some
    case End         => End.longValues.some
  }

  ///

  def apply(position: Long): Position              = Position(position, position)
  def apply(commit: Long, prepare: Long): Position = if (commit < 0 || prepare < 0) End else Exact(commit, prepare)

  implicit val orderForPosition: Order[Position] = Order.from {
    case (x: Exact, y: Exact) => Order[Exact].compare(x, y)
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

  implicit val orderForExact: Order[Exact] = Order.from { (x: Exact, y: Exact) =>
    (x.commit compare y.commit, x.prepare compare y.prepare) match {
      case (0, 0) => 0
      case (0, x) => x
      case (x, _) => x
    }
  }

  implicit val showForExact: Show[Exact] = Show.show[Exact] {
    case Exact(c, p) => s"Position(c = $c, p = $p)"
  }

  implicit val showForEventNumber: Show[Position] = Show.show[Position] {
    case e: Exact => e.show
    case End      => "end"
  }

}
