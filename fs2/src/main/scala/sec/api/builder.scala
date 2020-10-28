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

import scala.concurrent.duration._

import cats.Endo
import cats.data._
import cats.effect._
import cats.syntax.all._
import io.chrisdavenport.log4cats.Logger
import io.grpc.{ManagedChannel, ManagedChannelBuilder, NameResolverRegistry}
import sec.api.channel._
import sec.api.cluster._

//======================================================================================================================

sealed abstract class SingleNodeBuilder[F[_]] private (
  endpoint: Endpoint,
  authority: Option[String],
  options: Options,
  shutdownAwait: FiniteDuration,
  logger: Logger[F]
) extends OptionsBuilder[SingleNodeBuilder[F]] {

  private def copy(
    endpoint: Endpoint = endpoint,
    authority: Option[String] = authority,
    options: Options = options,
    shutdownAwait: FiniteDuration = shutdownAwait,
    logger: Logger[F] = logger
  ): SingleNodeBuilder[F] =
    new SingleNodeBuilder[F](endpoint, authority, options, shutdownAwait, logger) {}

  private[sec] def modOptions(fn: Endo[Options]): SingleNodeBuilder[F] = copy(options = fn(options))

  def withEndpoint(value: Endpoint): SingleNodeBuilder[F]                   = copy(endpoint = value)
  def withAuthority(value: String): SingleNodeBuilder[F]                    = copy(authority = value.some)
  def withNoAuthority: SingleNodeBuilder[F]                                 = copy(authority = None)
  def withLogger(value: Logger[F]): SingleNodeBuilder[F]                    = copy(logger = value)
  def withChannelShutdownAwait(await: FiniteDuration): SingleNodeBuilder[F] = copy(shutdownAwait = await)

  ///

  private[sec] def build[MCB <: ManagedChannelBuilder[MCB]](mcb: ChannelBuilderParams => F[MCB])(implicit
    F: ConcurrentEffect[F],
    T: Timer[F]
  ): Resource[F, EsClient[F]] = {

    val params: ChannelBuilderParams     = ChannelBuilderParams(endpoint, options.connectionMode)
    val channelBuilder: Resource[F, MCB] = Resource.liftF(mcb(params))
    val modifications: Endo[MCB]         = b => authority.fold(b)(a => b.overrideAuthority(a))

    channelBuilder >>= { builder =>
      modifications(builder).resource[F](shutdownAwait).map(EsClient[F](_, options, requiresLeader = false, logger))
    }
  }

}

object SingleNodeBuilder {

  private[sec] def apply[F[_]](
    endpoint: Endpoint,
    authority: Option[String],
    options: Options,
    shutdownAwait: FiniteDuration,
    logger: Logger[F]
  ): SingleNodeBuilder[F] =
    new SingleNodeBuilder[F](endpoint, authority, options, shutdownAwait, logger) {}
}

//======================================================================================================================

class ClusterBuilder[F[_]] private (
  seed: NonEmptySet[Endpoint],
  authority: String,
  options: Options,
  clusterOptions: ClusterOptions,
  logger: Logger[F]
) extends OptionsBuilder[ClusterBuilder[F]]
  with ClusterOptionsBuilder[ClusterBuilder[F]] {

  private def copy(
    seed: NonEmptySet[Endpoint] = seed,
    authority: String = authority,
    options: Options = options,
    clusterOptions: ClusterOptions = clusterOptions,
    logger: Logger[F] = logger
  ): ClusterBuilder[F] =
    new ClusterBuilder[F](seed, authority, options, clusterOptions, logger) {}

  private[sec] def modOptions(fn: Endo[Options]): ClusterBuilder[F]         = copy(options = fn(options))
  private[sec] def modCOptions(fn: Endo[ClusterOptions]): ClusterBuilder[F] = copy(clusterOptions = fn(clusterOptions))

  def withAuthority(value: String): ClusterBuilder[F] = copy(authority = value)
  def withLogger(value: Logger[F]): ClusterBuilder[F] = copy(logger = value)

  private[sec] def build[MCB <: ManagedChannelBuilder[MCB]](
    mcb: ChannelBuilderParams => F[MCB]
  )(implicit
    F: ConcurrentEffect[F],
    T: Timer[F]
  ): Resource[F, EsClient[F]] = {

    val log: Logger[F] = logger.withModifiedString(s => s"Cluster > $s")

    val builderForTarget: String => F[MCB] =
      t => mcb(ChannelBuilderParams(t, options.connectionMode))

    def gossipFn(mc: ManagedChannel): Gossip[F] = Gossip(
      EsClient.mkGossipFs2Grpc[F](mc),
      EsClient.mkContext(options, requiresLeader = false),
      EsClient.mkOpts[F](options.operationOptions.copy(retryEnabled = false), log, "gossip")
    )

    ClusterWatch(builderForTarget, clusterOptions, gossipFn, seed, authority, log) >>= { cw =>

      val mkProvider: Resource[F, ResolverProvider[F]] = ResolverProvider
        .bestNodes[F](authority, clusterOptions.preference, cw.subscribe, log)
        .evalTap[F, Unit](p => Sync[F].delay(NameResolverRegistry.getDefaultRegistry.register(p)))

      def builder(p: ResolverProvider[F]): Resource[F, MCB] =
        Resource.liftF(builderForTarget(s"${p.scheme}:///"))

      mkProvider >>= builder >>= {
        _.defaultLoadBalancingPolicy("round_robin")
          .overrideAuthority(authority)
          .resource[F](clusterOptions.channelShutdownAwait)
          .map(EsClient[F](_, options, clusterOptions.preference.isLeader, logger))
      }

    }

  }

}

object ClusterBuilder {

  private[sec] def apply[F[_]: ConcurrentEffect: Timer](
    seed: NonEmptySet[Endpoint],
    authority: String,
    options: Options,
    clusterOptions: ClusterOptions,
    logger: Logger[F]
  ): ClusterBuilder[F] =
    new ClusterBuilder[F](seed, authority, options, clusterOptions, logger)
}

//======================================================================================================================
