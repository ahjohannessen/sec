package sec
package api

import cats.implicits._
import cats.kernel.laws.discipline._
import org.scalacheck._
import org.specs2.mutable.Specification
import org.typelevel.discipline.specs2.mutable.Discipline
import sec.api.Direction._

class DirectionSpec extends Specification with Discipline {

  "Direction" >> {

    "Ops" >> {

      "fold" >> {
        Forwards.fold(ok, ko)
        Backwards.fold(ko, ok)
      }

    }

    "Eq" >> {
      implicit val arb: Arbitrary[Direction] = Arbitrary(Gen.oneOf(Forwards, Backwards))
      implicit val cogen: Cogen[Direction]   = Cogen[String].contramap[Direction](_.toString)
      checkAll("Direction", EqTests[Direction].eqv)
    }

  }

}
