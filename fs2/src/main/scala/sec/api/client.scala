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

import cats.{Applicative, Endo}
import cats.syntax.all._
import cats.data.NonEmptySet
import cats.effect.Resource
import cats.effect.kernel.Async
import com.eventstore.client.gossip.GossipFs2Grpc
import com.eventstore.client.streams.StreamsFs2Grpc
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import io.grpc.{CallOptions, ManagedChannel}
import fs2.grpc.client.ClientOptions
import sec.api.exceptions.{NotLeader, ServerUnavailable}
import sec.api.grpc.convert.convertToEs
import sec.api.grpc.metadata._
import sec.api.retries.RetryConfig
import com.eventstore.client.streams.BatchAppendReq
import fs2.concurrent.SignallingRef

trait EsClient[F[_]] {
  def streams: Streams[F]
  def metaStreams: MetaStreams[F]
  def gossip: Gossip[F]
}

object EsClient {

  def singleNode[F[_]: Applicative](endpoint: Endpoint): SingleNodeBuilder[F] =
    singleNode[F](endpoint, None, Options.default)

  private[sec] def singleNode[F[_]: Applicative](
    endpoint: Endpoint,
    authority: Option[String],
    options: Options
  ): SingleNodeBuilder[F] =
    SingleNodeBuilder[F](endpoint, authority, options, NoOpLogger.impl[F])

  def cluster[F[_]: Applicative](seed: NonEmptySet[Endpoint], authority: String): ClusterBuilder[F] =
    cluster[F](seed, authority, Options.default, ClusterOptions.default)

  private[sec] def cluster[F[_]: Applicative](
    seed: NonEmptySet[Endpoint],
    authority: String,
    options: Options,
    clusterOptions: ClusterOptions
  ): ClusterBuilder[F] =
    ClusterBuilder[F](seed, authority, options, clusterOptions, NoOpLogger.impl[F])

//======================================================================================================================

  private[sec] def apply[F[_]: Async](
    mc: ManagedChannel,
    options: Options,
    requiresLeader: Boolean,
    logger: Logger[F]
  ): Resource[F, EsClient[F]] = {

    def appender(s: StreamsFs2Grpc[F, Context], ctx: Context): Resource[F, Streams.BatchAppender[F]] =
      Streams.BatchAppender[F](s.batchAppend(_, ctx))

    for {
      s  <- mkStreamsFs2Grpc[F](mc, options.prefetchN)
      g  <- mkGossipFs2Grpc[F](mc)
      c   = mkContext(options, requiresLeader)(None)
      ba <- appender(s, c)

    } yield create[F](s, g, ba, options, requiresLeader, logger)

  }

  private[sec] def create[F[_]: Async](
    streamsFs2Grpc: StreamsFs2Grpc[F, Context],
    gossipFs2Grpc: GossipFs2Grpc[F, Context],
    batchAppender: Streams.BatchAppender[F],
    options: Options,
    requiresLeader: Boolean,
    logger: Logger[F]
  ): EsClient[F] = new EsClient[F] {

    val streams: Streams[F] = Streams(
      streamsFs2Grpc,
      mkContext(options, requiresLeader),
      options.batchAppendSize,
      batchAppender,
      mkOpts[F](options.operationOptions, logger, "Streams")
    )

    val metaStreams: MetaStreams[F] = MetaStreams[F](streams)

    val gossip: Gossip[F] = Gossip(
      gossipFs2Grpc,
      mkContext(options, requiresLeader),
      mkOpts[F](options.operationOptions, logger, "Gossip")
    )
  }

//======================================================================================================================

  private[sec] def mkContext(o: Options, requiresLeader: Boolean): Option[UserCredentials] => Context =
    uc => Context(o.connectionName, uc.orElse(o.credentials), requiresLeader)

  private[sec] val defaultRetryOn: Throwable => Boolean = {
    case _: ServerUnavailable | _: NotLeader => true
    case _                                   => false
  }

  private[sec] def mkOpts[F[_]](oo: OperationOptions, log: Logger[F], prefix: String): Opts[F] = {
    val rc = RetryConfig(oo.retryDelay, oo.retryMaxDelay, oo.retryBackoffFactor, oo.retryMaxAttempts, None)
    Opts[F](oo.retryEnabled, rc, defaultRetryOn, log.withModifiedString(s => s"$prefix > $s"))
  }

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

}
