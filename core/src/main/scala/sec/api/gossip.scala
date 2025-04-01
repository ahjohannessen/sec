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
import java.time.temporal.ChronoUnit.SECONDS
import java.util as ju
import cats.Order
import cats.implicits.*

/** Used for information about the nodes in an KurrentDB cluster.
  */
final case class ClusterInfo(
  members: Set[MemberInfo]
)

object ClusterInfo:

  given Order[ClusterInfo] = Order.by(_.members.toList.sorted)

  def renderClusterInfo(ci: ClusterInfo): String =
    val padTo   = ci.members.map(_.state.render).map(_.length).maxOption.getOrElse(0)
    val members = ci.members.toList.sorted
    s"ClusterInfo:\n${members.map(mi => s" ${MemberInfo.renderMemberInfo(mi, padTo)}").mkString("\n")}"

  extension (ci: ClusterInfo) def render: String = ClusterInfo.renderClusterInfo(ci)

final case class MemberInfo(
  instanceId: ju.UUID,
  timestamp: ZonedDateTime,
  state: VNodeState,
  isAlive: Boolean,
  httpEndpoint: Endpoint
)

object MemberInfo:

  given Order[MemberInfo] = Order.by(mi => (mi.httpEndpoint, mi.state, mi.isAlive, mi.instanceId))

  private[sec] def renderMemberInfo(mi: MemberInfo): String = renderMemberInfo(mi, 0)

  private[sec] def renderMemberInfo(mi: MemberInfo, padTo: Int): String =

    val alive    = s"${if (mi.isAlive) "✔" else "✕"}"
    val state    = s"${mi.state.render.padTo(padTo, ' ')}"
    val endpoint = s"${mi.httpEndpoint.render}"
    val ts       = s"${mi.timestamp.truncatedTo(SECONDS)}"
    val id       = s"${mi.instanceId}"

    s"$alive $state $endpoint $ts $id"

  extension (mi: MemberInfo) def render: String = MemberInfo.renderMemberInfo(mi)

sealed trait VNodeState
object VNodeState:

  case object Initializing extends VNodeState
  case object DiscoverLeader extends VNodeState
  case object Unknown extends VNodeState
  case object PreReplica extends VNodeState
  case object CatchingUp extends VNodeState
  case object Clone extends VNodeState
  case object Follower extends VNodeState
  case object PreLeader extends VNodeState
  case object Leader extends VNodeState
  case object Manager extends VNodeState
  case object ShuttingDown extends VNodeState
  case object Shutdown extends VNodeState
  case object ReadOnlyLeaderless extends VNodeState
  case object PreReadOnlyReplica extends VNodeState
  case object ReadOnlyReplica extends VNodeState
  case object ResigningLeader extends VNodeState

  private[sec] val values: Set[VNodeState] =
    Set(
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

  given Order[VNodeState] =
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

  extension (vns: VNodeState) def render: String = vns.toString
