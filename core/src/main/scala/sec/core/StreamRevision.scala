package sec
package core

import cats.{Eq, Show}

sealed trait StreamRevision
object StreamRevision {

  case object NoStream                            extends StreamRevision
  case object Any                                 extends StreamRevision
  case object StreamExists                        extends StreamRevision
  sealed abstract case class Version(value: Long) extends StreamRevision

  object Version {
    def apply(exact: EventNumber.Exact): Version = new Version(exact.revision) {}
  }

  implicit val eq: Eq[StreamRevision] = Eq.fromUniversalEquals
  implicit val showForAnyStreamRevision: Show[StreamRevision] = Show.show {
    case NoStream     => "NoStream"
    case Any          => "Any"
    case StreamExists => "StreamExists"
    case Version(v)   => s"Version($v)"
  }

}
