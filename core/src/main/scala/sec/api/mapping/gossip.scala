package sec
package api
package mapping

import cats.implicits._
import com.eventstore.client.{gossip => p}
import sec.api.mapping.shared._
import sec.api.mapping.time._
import sec.api.mapping.implicits._
import sec.api.Gossip._

private[sec] object gossip {

  val mkVNodeState: p.MemberInfo.VNodeState => Attempt[VNodeState] = s => {

    val result = s.asRecognized.map {
      case p.MemberInfo.VNodeState.Initializing       => VNodeState.Initializing
      case p.MemberInfo.VNodeState.DiscoverLeader     => VNodeState.DiscoverLeader
      case p.MemberInfo.VNodeState.Unknown            => VNodeState.Unknown
      case p.MemberInfo.VNodeState.PreReplica         => VNodeState.PreReplica
      case p.MemberInfo.VNodeState.CatchingUp         => VNodeState.CatchingUp
      case p.MemberInfo.VNodeState.Clone              => VNodeState.Clone
      case p.MemberInfo.VNodeState.Follower           => VNodeState.Follower
      case p.MemberInfo.VNodeState.PreLeader          => VNodeState.PreLeader
      case p.MemberInfo.VNodeState.Leader             => VNodeState.Leader
      case p.MemberInfo.VNodeState.Manager            => VNodeState.Manager
      case p.MemberInfo.VNodeState.ShuttingDown       => VNodeState.ShuttingDown
      case p.MemberInfo.VNodeState.Shutdown           => VNodeState.Shutdown
      case p.MemberInfo.VNodeState.ReadOnlyLeaderless => VNodeState.ReadOnlyLeaderless
      case p.MemberInfo.VNodeState.PreReadOnlyReplica => VNodeState.PreReadOnlyReplica
      case p.MemberInfo.VNodeState.ReadOnlyReplica    => VNodeState.ReadOnlyReplica
      case p.MemberInfo.VNodeState.ResigningLeader    => VNodeState.ResigningLeader
    }

    result.fold(s"Unrecognized state value ${s.value}".asLeft[VNodeState])(_.asRight)

  }

  def mkMemberInfo[F[_]: ErrorM](mi: p.MemberInfo): F[MemberInfo] = {

    val instanceId = mi.instanceId.require[F]("instanceId") >>= mkJuuid[F]
    val timestamp  = fromTicksSinceEpoch[F](mi.timeStamp)
    val state      = mkVNodeState(mi.state).leftMap(ProtoResultError).liftTo[F]
    val isAlive    = mi.isAlive.pure[F]
    val endpoint   = mi.httpEndPoint.require[F]("httpEndpoint").map(e => Endpoint(e.address, e.port))

    (instanceId, timestamp, state, isAlive, endpoint).mapN { (ii, ts, st, ia, ep) =>
      MemberInfo(ii, ts, st, ia, ep)
    }
  }

  def mkClusterInfo[F[_]: ErrorM](ci: p.ClusterInfo): F[ClusterInfo] =
    ci.members.toList.traverse(mkMemberInfo[F]).map(m => ClusterInfo(m.toSet))

}
