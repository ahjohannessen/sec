package sec
package api
package cluster

import cats.kernel.laws.discipline._
import org.scalacheck._
import org.specs2.mutable.Specification
import org.typelevel.discipline.specs2.mutable.Discipline
import NodePreference._

class NodePreferenceSpec extends Specification with Discipline {

  "Ops" >> {

    "isLeader" >> {
      Leader.isLeader should beTrue
    }
  }

  "Eq" >> {
    implicit val arb: Arbitrary[NodePreference] = Arbitrary(Gen.oneOf(Leader, Follower, ReadOnlyReplica))
    implicit val cogen: Cogen[NodePreference]   = Cogen[String].contramap[NodePreference](_.toString)
    checkAll("NodePreference", EqTests[NodePreference].eqv)
  }

}
