package sec
package api

import cats.Eq

//======================================================================================================================

sealed trait Direction
object Direction {
  case object Forwards  extends Direction
  case object Backwards extends Direction

  implicit final class DirectionOps(val d: Direction) extends AnyVal {
    def fold[A](fw: => A, bw: => A): A = d match {
      case Forwards  => fw
      case Backwards => bw
    }

  }

  implicit val eqForDirection: Eq[Direction] = Eq.fromUniversalEquals[Direction]

}

//======================================================================================================================
