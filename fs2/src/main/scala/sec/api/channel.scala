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

package sec.api

import java.io.{File, InputStream}
import scala.concurrent.duration._
import cats.syntax.all._
import cats.effect.{Resource, Sync}
import scodec.bits.ByteVector
import io.grpc._
import sec.api.ConnectionMode._

private[sec] object channel {

  def mkCredentials[F[_]: Sync](cm: ConnectionMode): F[Option[ChannelCredentials]] = {

    def from(cert: Either[File, InputStream]): F[ChannelCredentials] = Sync[F].blocking {
      val builder = TlsChannelCredentials.newBuilder()
      cert.fold(builder.trustManager, builder.trustManager).build()
    }

    def fromFile(cert: File): F[ChannelCredentials] = from(cert.asLeft)

    def fromB64(cert: CertB64): F[ChannelCredentials] = ByteVector
      .fromBase64Descriptive(cert.value)
      .map(_.toInputStream)
      .leftMap(new IllegalArgumentException(_))
      .liftTo[F]
      .flatMap(is => from(is.asRight))

    cm match {
      case Insecure     => none[ChannelCredentials].pure[F]
      case Secure(cert) => cert.fold(fromFile, fromB64).map(_.some)
    }

  }

  //

  def resource[F[_]: Sync](acquire: => ManagedChannel, shutdownAwait: FiniteDuration): Resource[F, ManagedChannel] = {
    resourceWithShutdown[F](acquire) { ch =>
      Sync[F].delay(ch.shutdown()) >>
        Sync[F].blocking {
          if (!ch.awaitTermination(shutdownAwait.length, shutdownAwait.unit)) {
            ch.shutdownNow()
            ()
          }
        }
    }
  }

  def resourceWithShutdown[F[_]: Sync](acquire: => ManagedChannel)(
    release: ManagedChannel => F[Unit]
  ): Resource[F, ManagedChannel] =
    Resource.make(Sync[F].delay(acquire))(release)

}
