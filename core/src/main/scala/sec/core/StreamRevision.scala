package sec
package core

import cats.{Eq, Show}

sealed trait StreamRevision
object StreamRevision {

  case object NoStream                          extends StreamRevision
  case object Any                               extends StreamRevision // Could be its own
  case object StreamExists                      extends StreamRevision // Need to find usecase for this
  sealed abstract case class Exact(value: Long) extends StreamRevision // This + NoStream can be fused in normal appendToStream usage

  object Exact {
    private[sec] def exact(value: Long): Exact       = new Exact(value) {}
    def apply(eventNumber: EventNumber.Exact): Exact = exact(eventNumber.value)
  }

  implicit val eq: Eq[StreamRevision] = Eq.fromUniversalEquals
  implicit val showForStreamRevision: Show[StreamRevision] = Show.show {
    case NoStream     => "NoStream"
    case Any          => "Any"
    case StreamExists => "StreamExists"
    case Exact(v)     => s"Exact($v)"
  }

}
