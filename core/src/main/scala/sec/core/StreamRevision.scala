package sec
package core

import cats.{Eq, Show}

sealed trait StreamRevision
object StreamRevision {

  case object NoStream                          extends StreamRevision
  case object Any                               extends StreamRevision
  case object StreamExists                      extends StreamRevision
  sealed abstract case class Exact(value: Long) extends StreamRevision

  object Exact {
    private[sec] def apply(value: Long): Exact       = new Exact(value) {}
    def apply(eventNumber: EventNumber.Exact): Exact = apply(eventNumber.value)
  }

  implicit val eq: Eq[StreamRevision] = Eq.fromUniversalEquals
  implicit val showForStreamRevision: Show[StreamRevision] = Show.show {
    case NoStream     => "NoStream"
    case Any          => "Any"
    case StreamExists => "StreamExists"
    case Exact(v)     => s"Exact($v)"
  }

}
