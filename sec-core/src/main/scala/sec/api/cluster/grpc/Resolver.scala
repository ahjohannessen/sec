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

import scala.jdk.CollectionConverters._
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect._
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import io.grpc.NameResolver
import io.grpc.NameResolver.{Listener2, ResolutionResult}
import sec.api.Gossip._
import sec.api.cluster.Notifier.Listener
import sec.api.cluster.grpc.Resolver.mkListener

final private[sec] case class Resolver[F[_]](
  authority: String,
  notifier: Notifier[F]
)(implicit F: Effect[F])
  extends NameResolver {
  override def start(l: Listener2): Unit   = F.toIO(notifier.start(mkListener[F](l)).allocated).void.unsafeRunSync()
  override val shutdown: Unit              = ()
  override val getServiceAuthority: String = authority
  override val refresh: Unit               = ()
}

private[sec] object Resolver {

  def mkResult(endpoints: NonEmptyList[Endpoint]): ResolutionResult =
    ResolutionResult.newBuilder().setAddresses(endpoints.toList.map(_.toEquivalentAddressGroup).asJava).build()

  def mkListener[F[_]](l2: Listener2)(implicit F: Sync[F]): Listener[F] =
    (endpoints: NonEmptyList[Endpoint]) => F.delay(l2.onResult(mkResult(endpoints)))

  def gossip[F[_]: ConcurrentEffect](
    authority: String,
    seed: NonEmptySet[Endpoint],
    updates: Stream[F, ClusterInfo],
    log: Logger[F]
  ): Resource[F, Resolver[F]] =
    Notifier.gossip[F](seed, updates, log).map(Resolver[F](authority, _))

  def bestNodes[F[_]: ConcurrentEffect](
    authority: String,
    np: NodePreference,
    updates: Stream[F, ClusterInfo],
    log: Logger[F]
  ): Resource[F, Resolver[F]] =
    Notifier.bestNodes[F](np, updates, log).map(Resolver[F](authority, _))

}
