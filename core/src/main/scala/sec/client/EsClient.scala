package sec
package client

import cats.Endo
import cats.data.NonEmptySet
import cats.effect.{ConcurrentEffect, Timer}
import com.eventstore.client.streams.StreamsFs2Grpc
import com.eventstore.client.gossip.GossipFs2Grpc
import io.grpc.{CallOptions, ManagedChannel}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.noop.NoOpLogger
import sec.core._
import sec.api._
import sec.api.grpc.metadata._
import sec.api.grpc.convert.convertToEs

trait EsClient[F[_]] {
  def streams: Streams[F]
  def gossip: Gossip[F]
}

object EsClient {

  def single[F[_]: ConcurrentEffect: Timer](endpoint: Endpoint): SingleNodeBuilder[F] =
    SingleNodeBuilder[F](endpoint, NoOpLogger.impl[F])

  def cluster[F[_]: ConcurrentEffect: Timer](seed: NonEmptySet[Endpoint], authority: String): ClusterBuilder[F] =
    ClusterBuilder[F](seed, authority, NoOpLogger.impl[F])

//======================================================================================================================

  private[sec] def apply[F[_]: ConcurrentEffect: Timer](
    mc: ManagedChannel,
    options: Options,
    requiresLeader: Boolean,
    logger: Logger[F]
  ): EsClient[F] = new EsClient[F] {

    val streams: Streams[F] = Streams(
      mkStreamsFs2Grpc[F](mc),
      mkContext(options, requiresLeader),
      mkOpts[F](options.operationOptions, logger, "Streams")
    )

    val gossip: Gossip[F] = Gossip(
      mkGossipFs2Grpc[F](mc),
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

  private[sec] def mkOpts[F[_]](oo: OperationOptions, log: Logger[F], prefix: String): Opts[F] =
    Opts[F](oo, defaultRetryOn, log.withModifiedString(s => s"$prefix: $s"))

  /// Streams

  private[sec] def mkStreamsFs2Grpc[F[_]: ConcurrentEffect](
    mc: ManagedChannel,
    fn: Endo[CallOptions] = identity
  ): StreamsFs2Grpc[F, Context] =
    StreamsFs2Grpc.client[F, Context](mc, _.toMetadata, fn, convertToEs)

  /// Gossip

  private[sec] def mkGossipFs2Grpc[F[_]: ConcurrentEffect](
    mc: ManagedChannel,
    fn: Endo[CallOptions] = identity
  ): GossipFs2Grpc[F, Context] =
    GossipFs2Grpc.client[F, Context](mc, _.toMetadata, fn, convertToEs)

}
