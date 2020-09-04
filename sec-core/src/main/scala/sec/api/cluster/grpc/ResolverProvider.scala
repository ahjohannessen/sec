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
package grpc

import java.net.URI
import io.grpc._
import io.grpc.NameResolver._
import cats.data.NonEmptySet
import cats.syntax.all._
import cats.effect._
import cats.effect.implicits._
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.chrisdavenport.log4cats.Logger
import sec.api.Gossip._

final private[sec] case class ResolverProvider[F[_]: Effect](
  scheme: String,
  resolver: F[Resolver[F]]
) extends NameResolverProvider {
  override val getDefaultScheme: String = scheme
  override val isAvailable: Boolean     = true
  override val priority: Int            = 4 // Less important than DNS

  override def newNameResolver(uri: URI, args: Args): NameResolver =
    if (scheme == uri.getScheme) resolver.toIO.unsafeRunSync() else null
}

object ResolverProvider {

  final val gossipScheme: String  = "eventstore-gossip"
  final val clusterScheme: String = "eventstore-cluster"

  private def mkHaltR[F[_]: Concurrent](log: Logger[F]): Resource[F, SignallingRef[F, Boolean]] =
    Resource.make(SignallingRef[F, Boolean](false)) { sr =>
      sr.set(true) *> log.debug("Signalled Notifier to shutdown.")
    }

  def gossip[F[_]: ConcurrentEffect](
    authority: String,
    seed: NonEmptySet[Endpoint],
    updates: Stream[F, ClusterInfo],
    log: Logger[F]
  ): Resource[F, ResolverProvider[F]] =
    mkHaltR[F](log.withModifiedString(s => s"Gossip Resolver > $s")).map { halt =>
      ResolverProvider(gossipScheme, Resolver.gossip(authority, seed, updates, halt))
    }

  def bestNodes[F[_]: ConcurrentEffect](
    authority: String,
    np: NodePreference,
    updates: Stream[F, ClusterInfo],
    log: Logger[F]
  ): Resource[F, ResolverProvider[F]] =
    mkHaltR[F](log.withModifiedString(s => s"BestNodes Resolver > $s")).map { halt =>
      ResolverProvider(clusterScheme, Resolver.bestNodes(authority, np, updates, halt))
    }

}
