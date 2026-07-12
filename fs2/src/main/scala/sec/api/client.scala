/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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

import cats.{Applicative, Endo}
import cats.syntax.all.*
import cats.data.NonEmptySet
import cats.effect.{Async, Resource}
import com.comcast.ip4s.{Hostname, Port}
import io.kurrent.dbclient.proto.gossip.GossipFs2Grpc
import io.kurrent.dbclient.proto.streams.{ReadReq, ReadResp, StreamsFs2Grpc}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import io.grpc.{CallOptions, ManagedChannel}
import fs2.Stream
import fs2.grpc.client.ClientOptions
import sec.api.pool.{GrpcChannelPool, GrpcSlot, Pool, SubscriptionPool}
import sec.api.exceptions.{NotLeader, ServerUnavailable}
import sec.api.grpc.convert.convertToEs
import sec.api.grpc.metadata.*
import sec.api.retries.RetryConfig
import sec.api.cluster.ClusterEndpoints
import sec.api.cluster.EndpointResolver
import cats.effect.kernel.Sync

trait EsClient[F[_]]:
  def streams: Streams[F]
  def metaStreams: MetaStreams[F]
  def gossip: Gossip[F]

object EsClient:

  def singleNode[F[_]: Applicative](
    endpoint: Endpoint
  ): SingleNodeBuilder[F] =
    singleNode[F](
      endpoint,
      None,
      Options.default
    )

  private[sec] def singleNode[F[_]: Applicative](
    endpoint: Endpoint,
    authority: Option[String],
    options: Options
  ): SingleNodeBuilder[F] =
    SingleNodeBuilder[F](
      endpoint,
      authority,
      options,
      NoOpLogger.impl[F]
    )

  def clusterViaDns[F[_]: Sync](
    clusterDns: Hostname,
    nodePort: Option[Port],
    authority: Option[String]
  ): ClusterBuilder[F] =

    val endpoints             = ClusterEndpoints.ViaDns(clusterDns)
    val authorityWithFallback = authority.getOrElse(clusterDns.toString)
    val options               = nodePort.fold(Options.default)(Options.default.withHttpPort)
    val clusterOptions        = ClusterOptions.default
    val endpointResolver      = EndpointResolver.default[F]

    cluster[F](endpoints, authorityWithFallback, options, clusterOptions)
      .withEndpointResolver(endpointResolver)

  def cluster[F[_]: Applicative](
    seed: NonEmptySet[Endpoint],
    authority: String
  ): ClusterBuilder[F] =

    val endpoints      = ClusterEndpoints.ViaSeed(seed)
    val options        = Options.default
    val clusterOptions = ClusterOptions.default

    cluster[F](endpoints, authority, options, clusterOptions)

  private[sec] def cluster[F[_]: Applicative](
    endpoints: ClusterEndpoints,
    authority: String,
    options: Options,
    clusterOptions: ClusterOptions
  ): ClusterBuilder[F] =
    ClusterBuilder[F](
      endpoints,
      authority,
      options,
      clusterOptions,
      NoOpLogger.impl[F],
      EndpointResolver.noop[F]
    )

//======================================================================================================================

  private[sec] def apply[F[_]: Async](
    mc: ManagedChannel,
    options: Options,
    requiresLeader: Boolean,
    logger: Logger[F],
    refreshHint: Option[Throwable => F[Unit]] = None,
    subscriptionPool: Option[SubscriptionPool[F]] = None
  ): Resource[F, EsClient[F]] =
    for
      s    <- mkStreamsFs2Grpc[F](mc, options.prefetchN)
      g    <- mkGossipFs2Grpc[F](mc)
      pool <- subscriptionPool.traverse { sp =>
                GrpcChannelPool.of[F](
                  sp.mkChannel,
                  mkStreamsFs2Grpc[F](_, options.prefetchN),
                  sp.config,
                  logger.withModifiedString(s => s"SubscriptionPool > $s")
                )
              }
    yield create[F](s, g, options, requiresLeader, logger, refreshHint, pool)

  private[sec] def create[F[_]: Async](
    streamsFs2Grpc: StreamsFs2Grpc[F, Context],
    gossipFs2Grpc: GossipFs2Grpc[F, Context],
    options: Options,
    requiresLeader: Boolean,
    logger: Logger[F],
    refreshHint: Option[Throwable => F[Unit]] = None,
    subscriptionPool: Option[Pool[F, GrpcSlot[F]]] = None
  ): EsClient[F] = new EsClient[F]:

    // The pool lease lives inside the per-attempt stream on purpose. Subscription retry
    // re-invokes this function on every attempt, and a failed attempt's stream finalizer
    // releases its permit before the next attempt acquires one - so a mass reconnect (e.g.
    // a server GOAWAY dropping every subscription at once) re-places subscriptions onto the
    // channels they just freed instead of growing the pool. A lease acquired outside this
    // function would hold its permit across attempts - a mass reconnect would then double
    // the demanded permits and grow the pool with channels that are never needed again -
    // and would also pin every retry to the channel picked at construction time, instead of
    // letting placement prefer healthier channels.
    private val subscriptionTransport: Option[(ReadReq, Context) => Stream[F, ReadResp]] =
      subscriptionPool.map { pool => (req, ctx) =>
        Stream.resource(pool.lease).flatMap(slot => slot.streams.read(req)(using ctx))
      }

    val streams: Streams[F] = Streams(
      streamsFs2Grpc,
      mkContext(options, requiresLeader),
      mkOpts[F](options.operationOptions, logger, "Streams", refreshHint),
      subscriptionTransport
    )

    val metaStreams: MetaStreams[F] = MetaStreams[F](streams)

    val gossip: Gossip[F] = Gossip(
      gossipFs2Grpc,
      mkContext(options, requiresLeader),
      mkOpts[F](options.operationOptions, logger, "Gossip", refreshHint)
    )

//======================================================================================================================

  private[sec] def mkContext(o: Options, requiresLeader: Boolean): Option[UserCredentials] => Context =
    uc => Context(o.connectionName, uc.orElse(o.credentials), requiresLeader)

  private[sec] val defaultRetryOn: Throwable => Boolean =
    case _: ServerUnavailable | _: NotLeader => true
    case _                                   => false

  private[sec] def mkOpts[F[_]](
    oo: OperationOptions,
    log: Logger[F],
    prefix: String,
    refreshHint: Option[Throwable => F[Unit]] = None
  ): Opts[F] =
    val rc = RetryConfig(oo.retryDelay, oo.retryMaxDelay, oo.retryBackoffFactor, oo.retryMaxAttempts, None)
    Opts[F](
      oo.retryEnabled,
      rc,
      defaultRetryOn,
      log.withModifiedString(s => s"$prefix > $s"),
      oo.subscriptionConfirmationTimeout,
      refreshHint
    )

  /// Streams

  private[sec] def mkStreamsFs2Grpc[F[_]: Async](
    mc: ManagedChannel,
    prefetchN: Int,
    fn: Endo[CallOptions] = identity
  ): Resource[F, StreamsFs2Grpc[F, Context]] =
    StreamsFs2Grpc.clientResource[F, Context](
      mc,
      _.toMetadata,
      ClientOptions.default
        .configureCallOptions(fn)
        .withErrorAdapter(Function.unlift(convertToEs))
        .withPrefetchN(prefetchN)
    )

  /// Gossip

  private[sec] def mkGossipFs2Grpc[F[_]: Async](
    mc: ManagedChannel,
    fn: Endo[CallOptions] = identity
  ): Resource[F, GossipFs2Grpc[F, Context]] =
    GossipFs2Grpc.clientResource[F, Context](
      mc,
      _.toMetadata,
      ClientOptions.default
        .configureCallOptions(fn)
        .withErrorAdapter(Function.unlift(convertToEs))
    )
