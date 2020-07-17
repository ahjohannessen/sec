package sec
package api

import cats.implicits._
import cats.kernel.laws.discipline._
import org.scalacheck._
import org.typelevel.discipline.specs2.mutable.Discipline
import sec.api.Gossip._
import Arbitraries._

class GossipITest extends ITest with Discipline {

  sequential

  "Gossip" >> {

    "read" >> {
      gossip
        .read(None)
        .map(_.members.headOption should beLike {
          case Some(MemberInfo(_, _, VNodeState.Leader, true, Endpoint("127.0.0.1", 2113))) => ok
        })
    }

    "ClusterInfo" >> {
      "order" >> {
        implicit val cogen: Cogen[ClusterInfo] = Cogen[String].contramap[ClusterInfo](_.toString)
        checkAll("ClusterInfo", OrderTests[ClusterInfo].eqv)
      }
    }

    "MemberInfo" >> {
      "order" >> {
        implicit val cogen: Cogen[MemberInfo] = Cogen[String].contramap[MemberInfo](_.toString)
        checkAll("MemberInfo", OrderTests[MemberInfo].order)
      }
    }

    "Endpoint" >> {
      "order" >> {
        implicit val cogen: Cogen[Endpoint] = Cogen[String].contramap[Endpoint](_.toString)
        checkAll("Endpoint", OrderTests[Endpoint].order)
      }
    }

    "VNodeState" >> {
      "order" >> {
        implicit val cogen: Cogen[VNodeState] = Cogen[String].contramap[VNodeState](_.toString)
        checkAll("VNodeState", OrderTests[VNodeState].order)
      }
    }

  }
}
