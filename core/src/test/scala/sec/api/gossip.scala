package sec
package api

import java.time.ZonedDateTime
import java.util.UUID
import cats.implicits._
import cats.kernel.laws.discipline._
import org.specs2.mutable.Specification
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

  }
}

class GossipSpec extends Specification with Discipline {

  "Gossip" >> {

    "ClusterInfo" >> {

      "order" >> {
        implicit val cogen: Cogen[ClusterInfo] = Cogen[String].contramap[ClusterInfo](_.toString)
        checkAll("ClusterInfo", OrderTests[ClusterInfo].eqv)
      }

      "show" >> {

        val m1 = MemberInfo(
          UUID.fromString("a781aa1f-0b48-47dd-84e1-519bdd1dcc6c"),
          ZonedDateTime.parse("2020-07-27T16:20:34Z"),
          VNodeState.Leader,
          isAlive = true,
          Endpoint("127.0.0.1", 2113)
        )

        val m2 = MemberInfo(
          UUID.fromString("7342793a-25cc-48b2-97fe-cd0779c043f3"),
          ZonedDateTime.parse("2020-07-27T16:20:34Z"),
          VNodeState.Follower,
          isAlive = false,
          Endpoint("127.0.0.2", 2113)
        )

        val m3 = MemberInfo(
          UUID.fromString("7f1ae876-d9d0-4441-b6c0-c5962e330dc6"),
          ZonedDateTime.parse("2020-07-27T16:20:34Z"),
          VNodeState.Follower,
          isAlive = true,
          Endpoint("127.0.0.3", 2113)
        )

        val ci = ClusterInfo(Set(m1, m2, m3))

        ci.show shouldEqual
          """ClusterInfo:
            | ✔ Leader   127.0.0.1:2113 2020-07-27T16:20:34Z a781aa1f-0b48-47dd-84e1-519bdd1dcc6c
            | ✕ Follower 127.0.0.2:2113 2020-07-27T16:20:34Z 7342793a-25cc-48b2-97fe-cd0779c043f3
            | ✔ Follower 127.0.0.3:2113 2020-07-27T16:20:34Z 7f1ae876-d9d0-4441-b6c0-c5962e330dc6""".stripMargin

      }

    }

    "MemberInfo" >> {

      "order" >> {
        implicit val cogen: Cogen[MemberInfo] = Cogen[String].contramap[MemberInfo](_.toString)
        checkAll("MemberInfo", OrderTests[MemberInfo].order)
      }

      "show" >> {

        val m1 = MemberInfo(
          UUID.fromString("a781aa1f-0b48-47dd-84e1-519bdd1dcc6c"),
          ZonedDateTime.parse("2020-07-27T16:20:34Z"),
          VNodeState.Follower,
          isAlive = true,
          Endpoint("127.0.0.1", 2113)
        )

        val m2 = MemberInfo(
          UUID.fromString("7f1ae876-d9d0-4441-b6c0-c5962e330dc6"),
          ZonedDateTime.parse("2020-07-27T16:20:34Z"),
          VNodeState.Leader,
          isAlive = false,
          Endpoint("127.0.0.2", 2113)
        )

        m1.show shouldEqual s"✔ Follower 127.0.0.1:2113 2020-07-27T16:20:34Z a781aa1f-0b48-47dd-84e1-519bdd1dcc6c"
        m2.show shouldEqual s"✕ Leader 127.0.0.2:2113 2020-07-27T16:20:34Z 7f1ae876-d9d0-4441-b6c0-c5962e330dc6"
      }
    }

    "Endpoint" >> {

      "order" >> {
        implicit val cogen: Cogen[Endpoint] = Cogen[String].contramap[Endpoint](_.toString)
        checkAll("Endpoint", OrderTests[Endpoint].order)
      }

      "show" >> {
        Endpoint("127.0.0.1", 2113).show shouldEqual "127.0.0.1:2113"
      }
    }

    "VNodeState" >> {

      "order" >> {
        implicit val cogen: Cogen[VNodeState] = Cogen[String].contramap[VNodeState](_.toString)
        checkAll("VNodeState", OrderTests[VNodeState].order)
      }

      "show" >> {
        VNodeState.values.map(_.toString) shouldEqual VNodeState.values.map(_.show)
      }
    }

  }
}
