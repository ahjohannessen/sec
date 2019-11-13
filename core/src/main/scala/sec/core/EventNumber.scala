package sec
package core

import cats.Order
import cats.implicits._

sealed trait EventNumber
object EventNumber {

  val Start: Exact = Exact.exact(0L)

  sealed abstract case class Exact(revision: Long) extends EventNumber
  object Exact {
    private[EventNumber] def exact(value: Long): Exact = new Exact(value) {}
    def apply(sr: StreamRevision.Exact): Exact         = exact(sr.value)
    def apply(value: Long): Option[Exact]              = if (value >= 0) exact(value).some else none
  }

  case object End extends EventNumber

  ///

  def apply(number: Long): EventNumber = if (number < 0) End else Exact.exact(number)

  implicit val orderForEventNumber: Order[EventNumber] = Order.from {
    case (x: Exact, y: Exact) => x.revision compare y.revision
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

}
