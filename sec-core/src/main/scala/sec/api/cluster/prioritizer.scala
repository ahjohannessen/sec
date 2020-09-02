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
package cluster

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.Sync
import sec.api.Gossip._
import sec.api.Gossip.VNodeState._

private[sec] object NodePrioritizer {

  final val notAllowedStates: Set[VNodeState] =
    Set(
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

  def pickBestNode[F[_]: Sync](
    members: NonEmptyList[MemberInfo],
    preference: NodePreference
  ): F[Option[MemberInfo]] = prioritizeNodes[F](members, preference).map(_.headOption)

  def prioritizeNodes[F[_]: Sync](
    members: NonEmptyList[MemberInfo],
    preference: NodePreference
  ): F[List[MemberInfo]] =
    prioritizeNodes[F](members, preference, (m: MemberInfo) => !notAllowedStates.contains(m.state))

  def prioritizeNodes[F[_]: Sync](
    members: NonEmptyList[MemberInfo],
    preference: NodePreference,
    allowed: MemberInfo => Boolean
  ): F[List[MemberInfo]] = {

    val candidates: List[MemberInfo] =
      members.filter(_.isAlive).filter(allowed).sortBy(_.state).reverse

    def arrange(p: MemberInfo => Boolean): F[List[MemberInfo]] = {
      val (satisfy, remaining) = candidates.partition(p)
      satisfy.shuffle[F].map(_ ::: remaining)
    }

    val isReadOnlyReplicaState: MemberInfo => Boolean =
      m => m.state.eqv(ReadOnlyLeaderless) || m.state.eqv(ReadOnlyReplica)

    preference match {
      case NodePreference.Leader          => arrange(_.state.eqv(Leader))
      case NodePreference.Follower        => arrange(_.state.eqv(Follower))
      case NodePreference.ReadOnlyReplica => arrange(isReadOnlyReplicaState)
    }
  }

}
