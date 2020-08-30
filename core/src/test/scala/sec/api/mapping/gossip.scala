package sec
package api
package mapping

import java.time.{Instant, ZoneOffset}
import java.util.{UUID => JUUID}
import cats.implicits._
import org.specs2._
import com.eventstore.client.UUID
import com.eventstore.client.{gossip => g}
import sec.api.Gossip._
import sec.api.Gossip.VNodeState._
import sec.api.mapping.gossip._

class GossipMappingSpec extends mutable.Specification {

  "mkVNodeState" >> {

    val expectations = Map[g.MemberInfo.VNodeState, VNodeState](
      g.MemberInfo.VNodeState.Initializing       -> Initializing,
      g.MemberInfo.VNodeState.DiscoverLeader     -> DiscoverLeader,
      g.MemberInfo.VNodeState.Unknown            -> Unknown,
      g.MemberInfo.VNodeState.PreReplica         -> PreReplica,
      g.MemberInfo.VNodeState.CatchingUp         -> CatchingUp,
      g.MemberInfo.VNodeState.Clone              -> Clone,
      g.MemberInfo.VNodeState.Follower           -> Follower,
      g.MemberInfo.VNodeState.PreLeader          -> PreLeader,
      g.MemberInfo.VNodeState.Leader             -> Leader,
      g.MemberInfo.VNodeState.Manager            -> Manager,
      g.MemberInfo.VNodeState.ShuttingDown       -> ShuttingDown,
      g.MemberInfo.VNodeState.Shutdown           -> Shutdown,
      g.MemberInfo.VNodeState.ReadOnlyLeaderless -> ReadOnlyLeaderless,
      g.MemberInfo.VNodeState.PreReadOnlyReplica -> PreReadOnlyReplica,
      g.MemberInfo.VNodeState.ReadOnlyReplica    -> ReadOnlyReplica,
      g.MemberInfo.VNodeState.ResigningLeader    -> ResigningLeader
    )

    expectations
      .map { case (p, d) => mkVNodeState(p) shouldEqual d.asRight }
      .reduce(_ and _)

    mkVNodeState(g.MemberInfo.VNodeState.Unrecognized(-1)) shouldEqual "Unrecognized state value -1".asLeft
  }

  "mkMemberInfo" >> {

    val instanceId = "e5390fcb-48bd-4895-bcc3-01629cca2af6"
    val timestamp  = Instant.EPOCH.atZone(ZoneOffset.UTC)
    val state      = g.MemberInfo.VNodeState.Leader
    val alive      = false
    val address    = "127.0.0.1"
    val port       = 2113

    val memberInfo = g
      .MemberInfo()
      .withInstanceId(UUID().withString(instanceId))
      .withTimeStamp(timestamp.getNano.toLong)
      .withState(state)
      .withIsAlive(alive)
      .withHttpEndPoint(g.EndPoint(address, port))

    // Happy Path
    mkMemberInfo[ErrorOr](memberInfo) shouldEqual
      MemberInfo(JUUID.fromString(instanceId), timestamp, Leader, alive, Endpoint(address, port)).asRight

    // Missing instanceId
    mkMemberInfo[ErrorOr](memberInfo.copy(instanceId = None)) shouldEqual
      ProtoResultError("Required value instanceId missing or invalid.").asLeft

    // Bad VNodeState
    mkMemberInfo[ErrorOr](memberInfo.withState(g.MemberInfo.VNodeState.Unrecognized(-1))) shouldEqual
      ProtoResultError("Unrecognized state value -1").asLeft

    // Missing Endpoint
    mkMemberInfo[ErrorOr](memberInfo.copy(httpEndPoint = None)) shouldEqual
      ProtoResultError("Required value httpEndpoint missing or invalid.").asLeft
  }

  "mkClusterInfo" >> {

    val instanceId = "e5390fcb-48bd-4895-bcc3-01629cca2af6"
    val timestamp  = Instant.EPOCH.atZone(ZoneOffset.UTC)
    val state      = g.MemberInfo.VNodeState.Leader
    val alive      = false
    val address    = "127.0.0.1"
    val port       = 2113

    val member = g
      .MemberInfo()
      .withInstanceId(UUID().withString(instanceId))
      .withTimeStamp(timestamp.getNano.toLong)
      .withState(state)
      .withIsAlive(alive)
      .withHttpEndPoint(g.EndPoint(address, port))

    mkClusterInfo[ErrorOr](g.ClusterInfo().withMembers(List(member))) should beRight(
      ClusterInfo(Set(MemberInfo(JUUID.fromString(instanceId), timestamp, Leader, alive, Endpoint(address, port))))
    )

    mkClusterInfo[ErrorOr](g.ClusterInfo().withMembers(Nil)) should beRight(ClusterInfo(Set.empty))

  }

}
