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

import cats.implicits._
import cats.effect.{Concurrent, Timer}
import com.eventstore.dbclient.proto.gossip.{GossipFs2Grpc, ClusterInfo => PClusterInfo}
import com.eventstore.dbclient.proto.shared.Empty
import sec.api.mapping.gossip.mkClusterInfo

trait Gossip[F[_]] {
  def read: F[ClusterInfo]
  def withCredentials(creds: UserCredentials): Gossip[F]
}

object Gossip {

  private[sec] def apply[F[_]: Concurrent: Timer, C](
    client: GossipFs2Grpc[F, C],
    mkCtx: Option[UserCredentials] => C,
    opts: Opts[F]
  ): Gossip[F] = new Gossip[F] {
    val read: F[ClusterInfo]                               = read0(opts)(client.read(Empty(), mkCtx(None)))
    def withCredentials(creds: UserCredentials): Gossip[F] = Gossip[F, C](client, _ => mkCtx(creds.some), opts)
  }

  private[sec] def read0[F[_]: Concurrent: Timer](o: Opts[F])(f: F[PClusterInfo]): F[ClusterInfo] =
    o.run(f, "read") >>= mkClusterInfo[F]

}
