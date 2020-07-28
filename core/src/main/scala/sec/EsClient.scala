package sec

import cats.Endo
import cats.effect.{ConcurrentEffect, Resource, Timer}
import fs2.Stream
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import com.eventstore.client.streams.StreamsFs2Grpc
import com.eventstore.client.gossip.GossipFs2Grpc
import io.grpc.{CallOptions, ManagedChannel, ManagedChannelBuilder}
import io.chrisdavenport.log4cats.Logger
import sec.core._
import sec.api._
import sec.api.grpc.metadata._
import sec.api.grpc.convert.convertToEs

trait EsClient[F[_]] {
  def streams: Streams[F]
  def gossip: Gossip[F]
}

object EsClient {

  private[sec] def stream[F[_]: ConcurrentEffect: Timer, MCB <: ManagedChannelBuilder[MCB]](
    builder: MCB,
    options: Options,
    logger: Logger[F]
  ): Stream[F, EsClient[F]] =
    Stream.resource(resource[F, MCB](builder, options, logger))

  private[sec] def resource[F[_]: ConcurrentEffect: Timer, MCB <: ManagedChannelBuilder[MCB]](
    builder: MCB,
    options: Options,
    logger: Logger[F]
  ): Resource[F, EsClient[F]] =
    builder.resource[F].map(apply[F](_, options, logger))

  private[sec] def apply[F[_]: ConcurrentEffect: Timer](
    mc: ManagedChannel,
    options: Options,
    l: Logger[F]
  ): EsClient[F] = new EsClient[F] {

    val streams: Streams[F] = Streams(
      mkStreamsClient[F](mc),
      mkContext(options, options.nodePreference.isLeader),
      mkStreamsOpts[F](options.operationOptions, l)
    )

    val gossip: Gossip[F] = Gossip(
      mkGossipClient[F](mc),
      mkContext(options, options.nodePreference.isLeader)
    )
  }

  ///

  private[sec] def mkContext(o: Options, requiresLeader: Boolean): Option[UserCredentials] => Context =
    uc => Context(o.connectionName, uc.orElse(o.defaultCreds), requiresLeader)

  /// Streams

  private[sec] val streamsRetryOn: Throwable => Boolean = {
    case _: ServerUnavailable | _: NotLeader => true
    case _                                   => false
  }

  private[sec] def mkStreamsClient[F[_]: ConcurrentEffect](
    mc: ManagedChannel,
    fn: Endo[CallOptions] = identity
  ): StreamsFs2Grpc[F, Context] =
    StreamsFs2Grpc.client[F, Context](mc, _.toMetadata, fn, convertToEs)

  private[sec] def mkStreamsOpts[F[_]](oo: OperationOptions, log: Logger[F]): Streams.Opts[F] =
    Streams.Opts[F](oo, streamsRetryOn, log.withModifiedString(s => s"Streams: $s"))

  /// Gossip

  private[sec] def mkGossipClient[F[_]: ConcurrentEffect](
    mc: ManagedChannel,
    fn: Endo[CallOptions] = identity
  ): GossipFs2Grpc[F, Context] =
    GossipFs2Grpc.client[F, Context](mc, _.toMetadata, fn, convertToEs)

}
