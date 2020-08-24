package sec
package client

import java.io.InputStream
import java.nio.file.Path
import cats.effect._
import cats.implicits._
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder.{forAddress, forTarget}
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts.{forClient}
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext

private[sec] object Netty {

  def mkBuilder[F[_]: Sync](p: ChannelBuilderParams): F[NettyChannelBuilder] = {

    def mkSslContext(tcc: Either[InputStream, Path]): F[SslContext] = Sync[F].delay {
      tcc.map(_.toFile).fold(forClient().trustManager, forClient().trustManager).build()
    }

    val ncb: NettyChannelBuilder =
      p.targetOrEndpoint.fold(forTarget, ep => forAddress(ep.address, ep.port))

    p.connectionMode match {
      case ConnectionMode.Insecure  => ncb.usePlaintext().pure[F]
      case ConnectionMode.Secure(v) => v.fold(ncb.useTransportSecurity().pure[F])(mkSslContext(_).map(ncb.sslContext))
    }
  }

}
