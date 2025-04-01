/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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
import cats.kernel.laws.discipline.*
import org.scalacheck.*
import sec.arbitraries.*

class GossipSuite extends SecDisciplineSuite:

  group("ClusterInfo") {

    test("order") {
      implicit val cogen: Cogen[ClusterInfo] = Cogen[String].contramap[ClusterInfo](_.toString)
      checkAll("ClusterInfo", OrderTests[ClusterInfo].eqv)
    }

    test("render") {

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

      assertEquals(
        ci.render,
        """ClusterInfo:
            | ✔ Leader   127.0.0.1:2113 2020-07-27T16:20:34Z a781aa1f-0b48-47dd-84e1-519bdd1dcc6c
            | ✕ Follower 127.0.0.2:2113 2020-07-27T16:20:34Z 7342793a-25cc-48b2-97fe-cd0779c043f3
            | ✔ Follower 127.0.0.3:2113 2020-07-27T16:20:34Z 7f1ae876-d9d0-4441-b6c0-c5962e330dc6""".stripMargin
      )

    }

  }

  group("MemberInfo") {

    test("order") {
      implicit val cogen: Cogen[MemberInfo] = Cogen[String].contramap[MemberInfo](_.toString)
      checkAll("MemberInfo", OrderTests[MemberInfo].order)
    }

    test("render") {

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

      assertEquals(m1.render, s"✔ Follower 127.0.0.1:2113 2020-07-27T16:20:34Z a781aa1f-0b48-47dd-84e1-519bdd1dcc6c")
      assertEquals(m2.render, s"✕ Leader 127.0.0.2:2113 2020-07-27T16:20:34Z 7f1ae876-d9d0-4441-b6c0-c5962e330dc6")
    }
  }

  group("VNodeState") {

    test("order") {
      implicit val cogen: Cogen[VNodeState] = Cogen[String].contramap[VNodeState](_.toString)
      checkAll("VNodeState", OrderTests[VNodeState].order)
    }

    test("render") {
      assertEquals(VNodeState.values.map(_.toString), VNodeState.values.map(_.render))
    }
  }
