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
package cluster

import java.net.URI
import scala.jdk.CollectionConverters._
import cats.data.{NonEmptyList, NonEmptySet}
import cats.syntax.all.*
import cats.effect.*
import fs2.Stream
import org.typelevel.log4cats.Logger
import io.grpc.NameResolver.{Args, Listener2, ResolutionResult}
import io.grpc.{NameResolver, NameResolverProvider}
import Notifier.Listener
import Resolver.mkListener
import cats.effect.std.Dispatcher

//======================================================================================================================

final private[sec] case class Resolver[F[_]: Sync](
  authority: String,
  notifier: Notifier[F],
  dispatcher: Dispatcher[F]
) extends NameResolver:
  override def start(l: Listener2): Unit   = dispatcher.unsafeRunSync(notifier.start(mkListener[F](l)).allocated.void)
  override val shutdown: Unit              = ()
  override val getServiceAuthority: String = authority
  override val refresh: Unit               = ()

private[sec] object Resolver:

  def mkResult(endpoints: NonEmptyList[Endpoint]): ResolutionResult =
    ResolutionResult.newBuilder().setAddresses(endpoints.toList.map(_.toEquivalentAddressGroup).asJava).build()

  def mkListener[F[_]](l2: Listener2)(implicit F: Sync[F]): Listener[F] =
    (endpoints: NonEmptyList[Endpoint]) => F.delay(l2.onResult(mkResult(endpoints)))

  def gossip[F[_]: Async](
    authority: String,
    seed: NonEmptySet[Endpoint],
    updates: Stream[F, ClusterInfo],
    log: Logger[F]
  ): Resource[F, Resolver[F]] = Dispatcher.parallel[F] >>= { d =>
    Notifier.gossip[F](seed, updates, log).map(Resolver[F](authority, _, d))
  }

  def bestNodes[F[_]: Async](
    authority: String,
    np: NodePreference,
    updates: Stream[F, ClusterInfo],
    log: Logger[F]
  ): Resource[F, Resolver[F]] = Dispatcher.parallel[F] >>= { d =>
    Notifier.bestNodes[F](np, updates, log).map(Resolver[F](authority, _, d))
  }

//======================================================================================================================

final private[sec] case class ResolverProvider[F[_]](
  scheme: String,
  resolver: Resolver[F]
) extends NameResolverProvider:
  override val getDefaultScheme: String = scheme
  override val isAvailable: Boolean     = true
  override val priority: Int            = 4 // Less important than DNS

  override def newNameResolver(uri: URI, args: Args): NameResolver =
    if scheme == uri.getScheme then resolver else null

private[sec] object ResolverProvider:

  final val gossipScheme: String  = "eventstore-gossip"
  final val clusterScheme: String = "eventstore-cluster"

  def gossip[F[_]: Async](
    authority: String,
    seed: NonEmptySet[Endpoint],
    updates: Stream[F, ClusterInfo],
    log: Logger[F]
  ): Resource[F, ResolverProvider[F]] = Resolver
    .gossip(authority, seed, updates, log.withModifiedString(s => s"Gossip Resolver > $s"))
    .map(ResolverProvider(gossipScheme, _))

  def bestNodes[F[_]: Async](
    authority: String,
    np: NodePreference,
    updates: Stream[F, ClusterInfo],
    log: Logger[F]
  ): Resource[F, ResolverProvider[F]] = Resolver
    .bestNodes(authority, np, updates, log.withModifiedString(s => s"BestNodes Resolver > $s"))
    .map(resolver => ResolverProvider(clusterScheme, resolver))

//======================================================================================================================
