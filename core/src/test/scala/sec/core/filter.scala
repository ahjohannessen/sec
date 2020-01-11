package sec
package core

import scala.util.matching.Regex
import cats.data.NonEmptyList
import cats.implicits._
import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import EventFilter._

class EventFilterSpec extends Specification with ScalaCheck {

  "EventFilter" >> {

    implicit val arbKind  = Arbitrary(Gen.oneOf[Kind](ByStreamId, ByEventType))
    implicit val arbMax   = Arbitrary(Gen.posNum[Int])
    implicit val arbRegex = Arbitrary(Gen.oneOf("^ctx1__.*".r, "^[^$].*".r))

    "prefix" >> prop { (k: Kind, msw: Option[Int], fst: String, rest: List[String]) =>
      prefix(k, msw, fst, rest: _*) shouldEqual
        EventFilter(k, msw, NonEmptyList(PrefixFilter(fst), rest.map(PrefixFilter)).asLeft)
    }

    "regex" >> prop { (k: Kind, msw: Option[Int], filter: Regex) =>
      regex(k, msw, filter.pattern.toString) shouldEqual EventFilter(k, msw, RegexFilter(filter).asRight)
    }

  }
}
