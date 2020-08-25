package sec
package client

import java.nio.file.Path
import cats.effect._
import cats.implicits._
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder.{forAddress, forTarget}
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts.forClient
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext

private[sec] object netty {

  def mkBuilder[F[_]: Sync](p: ChannelBuilderParams): F[NettyChannelBuilder] = {

    def mkSslContext(chainPath: Path): F[SslContext] = Sync[F].delay {
      forClient().trustManager(chainPath.toFile).build()
    }

    val ncb: NettyChannelBuilder =
      p.targetOrEndpoint.fold(forTarget, ep => forAddress(ep.address, ep.port))

    p.mode match {
      case ConnectionMode.Insecure  => ncb.usePlaintext().pure[F]
      case ConnectionMode.Secure(c) => mkSslContext(c).map(ncb.useTransportSecurity().sslContext)
    }

  }

}
