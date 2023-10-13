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

import cats.Eq
import cats.syntax.all.*

/** Used in conjunction with cluster connections where you provide a preference about what state a node should be in.
  *
  * There are three variants:
  *
  *   - [[NodePreference.Leader]] When you prefer the node you connect to is in a [[VNodeState.Leader]] state. This is,
  *     for instance, used when you wish to avoid unecessary network hops when appending data.
  *
  *   - [[NodePreference.Follower]] When you prefer the node you connect to is in a [[VNodeState.Follower]] state. This
  *     is useful in situations where you wish to get fast subscription updates for a read model.
  *
  *   - [[NodePreference.ReadOnlyReplica]] When you prefer the node you connect to is in a
  *     [[VNodeState.ReadOnlyReplica]] or [[VNodeState.ReadOnlyLeaderless]] state. This is useful when you wish to
  *     replicate data, but do not have requirements for fast updates or need to append data.
  */
sealed trait NodePreference
object NodePreference:

  case object Leader extends NodePreference
  case object Follower extends NodePreference
  case object ReadOnlyReplica extends NodePreference

  given Eq[NodePreference] = Eq.fromUniversalEquals[NodePreference]

  extension (np: NodePreference) def isLeader: Boolean = np.eqv(Leader)
