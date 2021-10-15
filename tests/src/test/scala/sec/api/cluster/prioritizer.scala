/*
 * Copyright 2020 Scala EventStoreDB Client
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
package cluster

import java.time.ZonedDateTime
import java.{util => ju}
import cats.data.{NonEmptyList => Nel}
// import cats.effect.testing.specs2._
import cats.syntax.all._
import org.scalacheck.{Arbitrary, Gen}
import org.specs2.mutable.Specification
import sec.arbitraries._
import VNodeState._

class NodePrioritizerSpec extends Specification with CatsEffect {

  import NodePrioritizer._

  "NodePrioritizer" should {

    def id         = sampleOf[ju.UUID]
    def ts         = sampleOf[ZonedDateTime]
    def port       = sampleOfGen(Gen.chooseNum(1, 65535))
    def address    = sampleOfGen(Gen.chooseNum(0, 254).map(i => s"127.0.0.$i"))
    val randomSeed = sampleOf(Arbitrary.arbLong)

    "allowed vnode states" >> {
      allowedStates shouldEqual Set(Leader, Follower, ReadOnlyReplica, ReadOnlyLeaderless)
      VNodeState.values.diff(allowedStates).size shouldEqual 12
    }

    "pick valid members" >> {

      val invalidStates = Nel.of[VNodeState](
        Manager,
        ShuttingDown,
        Manager,
        Shutdown,
        Unknown,
        Initializing,
        CatchingUp,
        ResigningLeader,
        ShuttingDown,
        PreLeader,
        PreReplica,
        PreReadOnlyReplica,
        Clone,
        DiscoverLeader
      )

      val invalidMembers = invalidStates.map(MemberInfo(id, ts, _, isAlive = true, Endpoint(address, port)))

      def test(ms: Nel[MemberInfo]) = pickBestNode(ms, NodePreference.Leader, randomSeed) should beNone

      invalidMembers.map(m => test(Nel.one(m)))
      test(invalidMembers)

    }

    "pick members based on preference" >> {

      val expectations: Map[NodePreference, VNodeState] = Map(
        NodePreference.Leader          -> Leader,
        NodePreference.Follower        -> Follower,
        NodePreference.ReadOnlyReplica -> ReadOnlyReplica,
        NodePreference.ReadOnlyReplica -> ReadOnlyLeaderless
      )

      def members(expected: VNodeState) =
        Nel.of(
          MemberInfo(id, ts, Leader, isAlive   = true, Endpoint(address, 1111)),
          MemberInfo(id, ts, Follower, isAlive = true, Endpoint(address, 2222)),
          MemberInfo(
            id,
            ts,
            if (expected.eqv(ReadOnlyReplica)) ReadOnlyReplica else ReadOnlyLeaderless,
            isAlive = true,
            Endpoint(address, 3333)
          ),
          MemberInfo(id, ts, Manager, isAlive = true, Endpoint(address, 4444))
        )

      def test(pref: NodePreference, expected: VNodeState) =
        pickBestNode(members(expected), pref, randomSeed).map(_.httpEndpoint.port) shouldEqual
          members(expected).filter(_.state.eqv(expected)).lastOption.map(_.httpEndpoint.port)

      expectations.toList.map { case (p, e) => test(p, e) }

    }

    "pick first alive member if preferred is not found" >> {

      val members =
        Nel.of(
          MemberInfo(id, ts, Leader, isAlive   = false, Endpoint(address, 1111)),
          MemberInfo(id, ts, Manager, isAlive  = true, Endpoint(address, 4444)),
          MemberInfo(id, ts, Follower, isAlive = true, Endpoint(address, 2222))
        )

      pickBestNode(members, NodePreference.Leader, randomSeed).map(_.httpEndpoint.port) shouldEqual
        members.filter(_.state.eqv(Follower)).lastOption.map(_.httpEndpoint.port)
    }

    "ListOps" >> {
      "shuffle is referentially transparent" >> {
        List(1, 2, 3).shuffle(1L) shouldEqual List(1, 2, 3).shuffle(1L)
      }
    }

  }

}
