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

import cats.kernel.laws.discipline.*
import org.scalacheck.*

class NodePreferenceSuite extends SecDisciplineSuite:
  import NodePreference.*

  group("Ops") {

    test("isLeader") {
      assert(Leader.isLeader)
    }

  }

  test("Eq") {
    implicit val arb: Arbitrary[NodePreference] = Arbitrary(Gen.oneOf(Leader, Follower, ReadOnlyReplica))
    implicit val cogen: Cogen[NodePreference]   = Cogen[String].contramap[NodePreference](_.toString)
    checkAll("NodePreference", EqTests[NodePreference].eqv)
  }
