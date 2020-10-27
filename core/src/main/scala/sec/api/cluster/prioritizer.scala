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

import VNodeState._

private[sec] object NodePrioritizer {

  val allowedStates: Set[VNodeState] =
    Set(Leader, Follower, ReadOnlyReplica, ReadOnlyLeaderless)

  def pickBestNode(
    members: NonEmptyList[MemberInfo],
    preference: NodePreference,
    randomSeed: Long
  ): Option[MemberInfo] = prioritizeNodes(members, preference, randomSeed).headOption

  def prioritizeNodes(
    members: NonEmptyList[MemberInfo],
    preference: NodePreference,
    randomSeed: Long
  ): List[MemberInfo] =
    prioritizeNodes(members, preference, randomSeed, (m: MemberInfo) => allowedStates.contains(m.state))

  def prioritizeNodes(
    members: NonEmptyList[MemberInfo],
    preference: NodePreference,
    randomSeed: Long,
    allowed: MemberInfo => Boolean
  ): List[MemberInfo] = {

    val candidates: List[MemberInfo] =
      members.filter(_.isAlive).filter(allowed).sortBy(_.state).reverse

    def arrange(p: MemberInfo => Boolean): List[MemberInfo] = {
      val (satisfy, remaining) = candidates.partition(p)
      satisfy.shuffle(randomSeed) ::: remaining
    }

    val isReadOnlyReplicaState: MemberInfo => Boolean =
      m => m.state.eqv(VNodeState.ReadOnlyLeaderless) || m.state.eqv(VNodeState.ReadOnlyReplica)

    preference match {
      case NodePreference.Leader          => arrange(_.state.eqv(VNodeState.Leader))
      case NodePreference.Follower        => arrange(_.state.eqv(VNodeState.Follower))
      case NodePreference.ReadOnlyReplica => arrange(isReadOnlyReplicaState)
    }
  }

//======================================================================================================================

  implicit final private[sec] class ListOps[A](val inner: List[A]) extends AnyVal {
    def shuffle(seed: Long): List[A] = new scala.util.Random(seed).shuffle(inner)
  }

}
