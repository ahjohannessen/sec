package sec
package core

import cats.Order

sealed trait Position
object Position {

  val Start: Exact = exact(0L, 0L)
  sealed abstract case class Exact(commit: Long, prepare: Long) extends Position
  case object End                                               extends Position

  private[sec] def exact(commit: Long, prepare: Long): Exact = new Exact(commit, prepare) {}

  ///

  def apply(position: Long): Position              = Position(position, position)
  def apply(commit: Long, prepare: Long): Position = if (commit < 0 || prepare < 0) End else exact(commit, prepare)

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

}
