package sec
package api
package cluster

import java.{util => ju}
import java.time.ZonedDateTime
import org.specs2.mutable.Specification
import org.scalacheck.Gen
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import cats.effect.testing.specs2.CatsIO
import cats.effect._
import Gossip._
import Gossip.VNodeState._
import Arbitraries._

class NodePickerSpec extends Specification with CatsIO {

  "NodePrioritizer" should {

    def id      = sampleOf[ju.UUID]
    def ts      = sampleOf[ZonedDateTime]
    def port    = sampleOfGen(Gen.chooseNum(1, 65535))
    def address = sampleOfGen(Gen.chooseNum(0, 255).map(i => s"127.0.0.$i"))

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

      val invalidMembers = invalidStates.map(MemberInfo(id, ts, _, true, Endpoint(address, port)))

      def test(ms: Nel[MemberInfo]) =
        NodePrioritizer.pickBestNode[IO](ms, NodePreference.Leader).map(_ should beNone)

      invalidMembers.traverse(m => test(Nel.one(m))) *> test(invalidMembers)

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
            if (expected === ReadOnlyReplica) ReadOnlyReplica else ReadOnlyLeaderless,
            isAlive = true,
            Endpoint(address, 3333)
          ),
          MemberInfo(id, ts, Manager, isAlive = true, Endpoint(address, 4444))
        )

      def test(pref: NodePreference, expected: VNodeState) =
        NodePrioritizer
          .pickBestNode[IO](members(expected), pref)
          .map(_.map(_.httpEndpoint.port))
          .map(_ shouldEqual members(expected).filter(_.state === expected).lastOption.map(_.httpEndpoint.port))

      expectations.toList.traverse {
        case (p, e) => test(p, e)
      }

    }

    "pick first alive member if preferred is not found" >> {

      val members =
        Nel.of(
          MemberInfo(id, ts, Leader, isAlive   = false, Endpoint(address, 1111)),
          MemberInfo(id, ts, Manager, isAlive  = true, Endpoint(address, 4444)),
          MemberInfo(id, ts, Follower, isAlive = true, Endpoint(address, 2222))
        )

      NodePrioritizer
        .pickBestNode[IO](members, NodePreference.Leader)
        .map(_.map(_.httpEndpoint.port))
        .map(_ shouldEqual members.filter(_.state === Follower).lastOption.map(_.httpEndpoint.port))
    }

  }

}
