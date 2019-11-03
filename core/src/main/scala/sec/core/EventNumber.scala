package sec
package core

import cats.Order

sealed trait EventNumber
object EventNumber {

  val Start: Exact = exact(0L)
  sealed abstract case class Exact(revision: Long) extends EventNumber
  case object End                                  extends EventNumber

  private def exact(number: Long): Exact = new Exact(number) {}

  ///

  def apply(number: Long): EventNumber = if (number < 0) End else exact(number)

  implicit val orderForEventNumber: Order[EventNumber] = Order.from {
    case (x: Exact, y: Exact) => x.revision compare y.revision
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

}
