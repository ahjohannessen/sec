package sec

import cats.effect.{ConcurrentEffect, Resource, Timer}
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import com.eventstore.client.streams.StreamsFs2Grpc
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import fs2.Stream
import sec.api._
import sec.api.grpc.implicits._
import sec.api.grpc.convert.convertToEs

trait EsClient[F[_]] {
  def streams: Streams[F]
}

object EsClient {

  private[sec] def stream[F[_]: ConcurrentEffect: Timer, MCB <: ManagedChannelBuilder[MCB]](
    builder: MCB,
    options: Options
  ): Stream[F, EsClient[F]] =
    Stream.resource(resource[F, MCB](builder, options))

  private[sec] def resource[F[_]: ConcurrentEffect: Timer, MCB <: ManagedChannelBuilder[MCB]](
    builder: MCB,
    options: Options
  ): Resource[F, EsClient[F]] =
    builder.resource[F].map(apply[F](_, options))

  private[sec] def apply[F[_]: ConcurrentEffect: Timer](mc: ManagedChannel, options: Options): EsClient[F] =
    new Impl[F](mc, options)

  private final class Impl[F[_]: ConcurrentEffect: Timer](mc: ManagedChannel, options: Options) extends EsClient[F] {
    val streams: Streams[F] =
      Streams(StreamsFs2Grpc.client[F, Context](mc, _.toMetadata, identity, convertToEs), options)
  }

}
