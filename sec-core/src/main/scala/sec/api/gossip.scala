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

import java.{util => ju}
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit.SECONDS
import cats._
import cats.implicits._
import cats.effect.{Concurrent, Timer}
import com.eventstore.client.gossip.{GossipFs2Grpc, ClusterInfo => PClusterInfo}
import com.eventstore.client.Empty
import mapping.gossip.mkClusterInfo

trait Gossip[F[_]] {
  def read(creds: Option[UserCredentials]): F[Gossip.ClusterInfo]
}

object Gossip {

  private[sec] def apply[F[_]: Concurrent: Timer, C](
    client: GossipFs2Grpc[F, C],
    mkCtx: Option[UserCredentials] => C,
    opts: Opts[F]
  ): Gossip[F] = new Gossip[F] {
    def read(creds: Option[UserCredentials]): F[ClusterInfo] = read0(opts)(client.read(Empty(), mkCtx(creds)))
  }

  private[sec] def read0[F[_]: Concurrent: Timer](o: Opts[F])(f: F[PClusterInfo]): F[ClusterInfo] =
    o.run(f, "read") >>= mkClusterInfo[F]

//======================================================================================================================

  final case class ClusterInfo(
    members: Set[MemberInfo]
  )

  object ClusterInfo {
    implicit val orderForClusterInfo: Order[ClusterInfo] = Order.by(_.members.toList.sorted)
    implicit val showForClusterInfo: Show[ClusterInfo] = Show.show { ci =>
      val padTo   = ci.members.map(_.state.show).map(_.length).maxOption.getOrElse(0)
      val members = ci.members.toList.sorted
      s"ClusterInfo:\n${members.map(mi => s" ${MemberInfo.mkShow(padTo).show(mi)}").mkString("\n")}"
    }
  }

  final case class MemberInfo(
    instanceId: ju.UUID,
    timestamp: ZonedDateTime,
    state: VNodeState,
    isAlive: Boolean,
    httpEndpoint: Endpoint
  )

  object MemberInfo {

    implicit val orderForMemberInfo: Order[MemberInfo] =
      Order.by(mi => (mi.httpEndpoint, mi.state, mi.isAlive, mi.instanceId))

    implicit val showForMemberInfo: Show[MemberInfo] = mkShow(0)

    private[sec] def mkShow(padTo: Int): Show[MemberInfo] = Show.show { mi =>

      val alive    = s"${mi.isAlive.fold("✔", "✕")}"
      val state    = s"${mi.state.show.padTo(padTo, ' ')}"
      val endpoint = s"${mi.httpEndpoint.show}"
      val ts       = s"${mi.timestamp.truncatedTo(SECONDS)}"
      val id       = s"${mi.instanceId}"

      s"$alive $state $endpoint $ts $id"
    }

  }

  sealed trait VNodeState
  object VNodeState {

    case object Initializing       extends VNodeState
    case object DiscoverLeader     extends VNodeState
    case object Unknown            extends VNodeState
    case object PreReplica         extends VNodeState
    case object CatchingUp         extends VNodeState
    case object Clone              extends VNodeState
    case object Follower           extends VNodeState
    case object PreLeader          extends VNodeState
    case object Leader             extends VNodeState
    case object Manager            extends VNodeState
    case object ShuttingDown       extends VNodeState
    case object Shutdown           extends VNodeState
    case object ReadOnlyLeaderless extends VNodeState
    case object PreReadOnlyReplica extends VNodeState
    case object ReadOnlyReplica    extends VNodeState
    case object ResigningLeader    extends VNodeState

    final private[sec] val values: List[VNodeState] = List(
      Initializing,
      DiscoverLeader,
      Unknown,
      PreReplica,
      CatchingUp,
      Clone,
      Follower,
      PreLeader,
      Leader,
      Manager,
      ShuttingDown,
      Shutdown,
      ReadOnlyLeaderless,
      PreReadOnlyReplica,
      ReadOnlyReplica,
      ResigningLeader
    )

    implicit val orderForVNodeState: Order[VNodeState] =
      Order.by[VNodeState, Int] {
        case Initializing       => 0
        case DiscoverLeader     => 1
        case Unknown            => 2
        case PreReplica         => 3
        case CatchingUp         => 4
        case Clone              => 5
        case Follower           => 6
        case PreLeader          => 7
        case Leader             => 8
        case Manager            => 9
        case ShuttingDown       => 10
        case Shutdown           => 11
        case ReadOnlyLeaderless => 12
        case PreReadOnlyReplica => 13
        case ReadOnlyReplica    => 14
        case ResigningLeader    => 15
      }

    implicit val showForVNodeState: Show[VNodeState] = Show.fromToString[VNodeState]

  }

}
