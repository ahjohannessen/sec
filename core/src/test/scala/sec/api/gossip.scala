/*
 * Copyright 2020 Alex Henning Johannessen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sec
package api

import java.time.ZonedDateTime
import java.util.UUID
import cats.syntax.all._
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
