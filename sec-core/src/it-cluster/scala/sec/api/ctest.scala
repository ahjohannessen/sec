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

import java.nio.file.Path
import java.io.File
import cats.data.NonEmptySet
import cats.implicits._
import cats.effect._
import org.specs2.mutable.Specification
import cats.effect.testing.specs2._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.grpc.netty.NettyChannelBuilder
import io.grpc.netty.NettyChannelBuilder.{forAddress, forTarget}
import io.grpc.netty.GrpcSslContexts.forClient
import io.netty.handler.ssl.SslContext
import helpers.text.snakeCaseTransformation
import helpers.endpoint.endpointFrom
import sec.client._

trait CTest extends Specification with CatsResourceIO[(EsClient[IO], Logger[IO])] {

  import CTest._

  final private val testName    = snakeCaseTransformation(getClass.getSimpleName)
  final private val certsFolder = new File(sys.env.getOrElse("SEC_CLUSTER_CERTS_PATH", TestBuildInfo.certsPath))
  final private val ca          = new File(certsFolder, "ca/ca.crt")
  final private val authority   = sys.env.getOrElse("SEC_CLUSTER_AUTHORITY", "es.sec.local")
  final private val seed = NonEmptySet.of(
    endpointFrom("SEC_CLUSTER_ES1_ADDRESS", "SEC_CLUSTER_ES1_PORT", "127.0.0.1", 2114),
    endpointFrom("SEC_CLUSTER_ES2_ADDRESS", "SEC_CLUSTER_ES2_PORT", "127.0.0.1", 2115),
    endpointFrom("SEC_CLUSTER_ES3_ADDRESS", "SEC_CLUSTER_ES3_PORT", "127.0.0.1", 2116)
  )

  ///

  def resource: Resource[IO, (EsClient[IO], Logger[IO])] = for {
    log    <- Resource.liftF(Slf4jLogger.fromName[IO](testName))
    client <- EsClient.cluster[IO](seed, authority).withCertificate(ca.toPath).withLogger(log).build(mkBuilder[IO])
  } yield (client, log)

}

object CTest {

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
