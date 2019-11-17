package sec
package core

import scala.util.matching.Regex
import EventFilter._

// TODO: combine prefixes into
// `expression: Either[NonEmptyList[Prefix], Regex]`

sealed abstract case class EventFilter(
  prefixes: List[PrefixFilterExpression],
  regex: Option[RegularFilterExpression],
  maxSearchWindow: Option[Int],
  kind: Kind
)

object EventFilter {

  sealed trait Kind
  case object Stream    extends Kind
  case object EventType extends Kind

  // smart constructors for valid combinations

  ///

  sealed trait Expression
  final case class PrefixFilterExpression(value: String)  extends Expression
  final case class RegularFilterExpression(value: String) extends Expression

  object RegularFilterExpression {
    val excludeSystemEvents                          = RegularFilterExpression("""^[^$].*""".r)
    def apply(regex: Regex): RegularFilterExpression = RegularFilterExpression(regex.pattern.toString)
  }

}
