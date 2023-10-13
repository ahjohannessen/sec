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
package mapping

import java.time.{Instant, ZoneOffset}
import java.util.UUID as JUUID
import cats.syntax.all.*
import com.eventstore.dbclient.proto.shared.UUID
import com.eventstore.dbclient.proto.gossip as g
import sec.api.mapping.gossip.*

class GossipMappingSuite extends SecSuite:
  import VNodeState.*

  test("mkVNodeState") {

    assertEquals(
      mkVNodeState(g.MemberInfo.VNodeState.Unrecognized(-1)),
      "Unrecognized state value -1".asLeft
    )

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

    expectations.map { case (p, d) =>
      assertEquals(mkVNodeState(p), d.asRight)
    }

  }

  test("mkMemberInfo") {

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
    assertEquals(
      mkMemberInfo[ErrorOr](memberInfo),
      MemberInfo(JUUID.fromString(instanceId), timestamp, Leader, alive, Endpoint(address, port)).asRight
    )

    // Missing instanceId
    assertEquals(
      mkMemberInfo[ErrorOr](memberInfo.copy(instanceId = None)),
      ProtoResultError("Required value instanceId missing or invalid.").asLeft
    )

    // Bad VNodeState
    assertEquals(
      mkMemberInfo[ErrorOr](memberInfo.withState(g.MemberInfo.VNodeState.Unrecognized(-1))),
      ProtoResultError("Unrecognized state value -1").asLeft
    )

    // Missing Endpoint
    assertEquals(
      mkMemberInfo[ErrorOr](memberInfo.copy(httpEndPoint = None)),
      ProtoResultError("Required value httpEndpoint missing or invalid.").asLeft
    )

  }

  test("mkClusterInfo") {

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

    assertEquals(
      mkClusterInfo[ErrorOr](g.ClusterInfo().withMembers(List(member))),
      ClusterInfo(
        Set(MemberInfo(JUUID.fromString(instanceId), timestamp, Leader, alive, Endpoint(address, port)))
      ).asRight
    )

    assertEquals(
      mkClusterInfo[ErrorOr](g.ClusterInfo().withMembers(Nil)),
      ClusterInfo(Set.empty).asRight
    )

  }
