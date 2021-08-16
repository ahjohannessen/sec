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

import cats.Endo
import cats.data._
import cats.effect._
import cats.syntax.all._
import org.typelevel.log4cats.Logger
import io.grpc.{ManagedChannel, ManagedChannelBuilder, NameResolverRegistry}
import sec.api.channel._
import sec.api.cluster._
import fs2.concurrent.SignallingRef

//======================================================================================================================

sealed abstract class SingleNodeBuilder[F[_]] private (
  val endpoint: Endpoint,
  val authority: Option[String],
  options: Options,
  logger: Logger[F]
) extends OptionsBuilder[SingleNodeBuilder[F]] {

  private def copy(
    endpoint: Endpoint = endpoint,
    authority: Option[String] = authority,
    options: Options = options,
    logger: Logger[F] = logger
  ): SingleNodeBuilder[F] =
    new SingleNodeBuilder[F](endpoint, authority, options, logger) {}

  private[sec] def modOptions(fn: Endo[Options]): SingleNodeBuilder[F] = copy(options = fn(options))

  def withEndpoint(value: Endpoint): SingleNodeBuilder[F] = copy(endpoint = value)
  def withAuthority(value: String): SingleNodeBuilder[F]  = copy(authority = value.some)
  def withNoAuthority: SingleNodeBuilder[F]               = copy(authority = None)
  def withLogger(value: Logger[F]): SingleNodeBuilder[F]  = copy(logger = value)

  private[sec] def build[MCB <: ManagedChannelBuilder[MCB]](mcb: ChannelBuilderParams => F[MCB])(implicit
    F: Async[F]): Resource[F, EsClient[F]] = {

    val mkChannelBuilderParams: F[ChannelBuilderParams] =
      mkCredentials(options.connectionMode).map(ChannelBuilderParams(endpoint, _))

    val makeClient: MCB => Resource[F, EsClient[F]] = { builder =>

      val mods: Endo[MCB] =
        b => authority.fold(b)(a => b.overrideAuthority(a))

      channel
        .resource[F](mods(builder).build, options.channelShutdownAwait)
        .flatMap(EsClient[F](_, options, requiresLeader = false, logger))
    }

    Resource.eval(mkChannelBuilderParams).evalMap(mcb) >>= makeClient
  }

}

object SingleNodeBuilder {

  private[sec] def apply[F[_]](
    endpoint: Endpoint,
    authority: Option[String],
    options: Options,
    logger: Logger[F]
  ): SingleNodeBuilder[F] =
    new SingleNodeBuilder[F](endpoint, authority, options, logger) {}
}

//======================================================================================================================

class ClusterBuilder[F[_]] private (
  val seed: NonEmptySet[Endpoint],
  val authority: String,
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
  )(implicit F: Async[F]): Resource[F, EsClient[F]] = {

    val log: Logger[F] = logger.withModifiedString(s => s"Cluster > $s")

    val builderForTarget: String => F[MCB] = t =>
      mkCredentials(options.connectionMode) >>= { cc => mcb(ChannelBuilderParams(t, cc)) }

    def gossipFn(mc: ManagedChannel): Resource[F, Gossip[F]] = EsClient
      .mkGossipFs2Grpc[F](mc)
      .map(g =>
        Gossip(g,
               EsClient.mkContext(options, requiresLeader = false),
               EsClient.mkOpts[F](options.operationOptions.copy(retryEnabled = false), log, "gossip")))

    ClusterWatch(builderForTarget, options, clusterOptions, gossipFn, seed, authority, log) >>= { cw =>

      val mkProvider: Resource[F, ResolverProvider[F]] = ResolverProvider
        .bestNodes[F](authority, clusterOptions.preference, cw.subscribe, log)
        .evalTap(p => Sync[F].delay(NameResolverRegistry.getDefaultRegistry.register(p)))

      def builder(p: ResolverProvider[F]): Resource[F, MCB] =
        Resource.eval(builderForTarget(s"${p.scheme}:///"))

      val mods: Endo[MCB] =
        _.defaultLoadBalancingPolicy("round_robin").overrideAuthority(authority)

      mkProvider >>= builder >>= { b =>
        channel.resource[F](mods(b).build, options.channelShutdownAwait) >>= { mc =>
          EsClient[F](mc, options, clusterOptions.preference.isLeader, logger)
        }
      }

    }

  }

}

object ClusterBuilder {

  private[sec] def apply[F[_]](
    seed: NonEmptySet[Endpoint],
    authority: String,
    options: Options,
    clusterOptions: ClusterOptions,
    logger: Logger[F]
  ): ClusterBuilder[F] =
    new ClusterBuilder[F](seed, authority, options, clusterOptions, logger)
}

//======================================================================================================================
