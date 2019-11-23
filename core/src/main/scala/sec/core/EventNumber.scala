package sec
package core

import cats.Order
import cats.implicits._

sealed trait EventNumber
object EventNumber {

  val Start: Exact = Exact(0L)

  sealed abstract case class Exact(value: Long) extends EventNumber
  object Exact {
    private[sec] def apply(value: Long): Exact = new Exact(value) {}
    def apply(sr: StreamRevision.Exact): Exact = apply(sr.value)
    def opt(value: Long): Option[Exact]        = if (value >= 0) apply(value).some else none

    implicit final class ExactOps(e: Exact) {
      def asRevision: StreamRevision = StreamRevision.Exact(e)
    }

  }

  case object End extends EventNumber

  ///

  def apply(number: Long): EventNumber = if (number < 0) End else Exact(number)

  implicit val orderForEventNumber: Order[EventNumber] = Order.from {
    case (x: Exact, y: Exact) => x.value compare y.value
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

}
