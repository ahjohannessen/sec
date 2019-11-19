package sec
package core

import scala.util.matching.Regex
import cats.implicits._
import cats.data.NonEmptyList
import EventFilter._

final case class EventFilter(
  kind: Kind,
  maxSearchWindow: Option[Int],
  option: Either[NonEmptyList[PrefixFilter], RegexFilter]
)

object EventFilter {

  sealed trait Kind
  case object Stream    extends Kind
  case object EventType extends Kind

  def prefix(kind: Kind, maxSearchWindow: Option[Int], fst: PrefixFilter, rest: PrefixFilter*): EventFilter =
    EventFilter(kind, maxSearchWindow, NonEmptyList(fst, rest.toList).asLeft)

  def regex(kind: Kind, maxSearchWindow: Option[Int], filter: RegexFilter): EventFilter =
    EventFilter(kind, maxSearchWindow, filter.asRight)

  ///

  sealed trait Expression
  final case class PrefixFilter(value: String) extends Expression
  final case class RegexFilter(value: String)  extends Expression

  object RegexFilter {
    val excludeSystemEvents: Regex       = "^[^$].*".r
    def apply(regex: Regex): RegexFilter = RegexFilter(regex.pattern.toString)
  }

}
