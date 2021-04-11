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

import java.io.File

import scala.concurrent.blocking
import scala.concurrent.duration._

import cats.syntax.all._
import cats.effect.{Resource, Sync}
import io.grpc._
import sec.api.ConnectionMode._

private[sec] object channel {

  def mkCredentials[F[_]: Sync](cm: ConnectionMode): F[Option[ChannelCredentials]] = {

    def make(cert: File): F[ChannelCredentials] = Sync[F].blocking {
      TlsChannelCredentials.newBuilder().trustManager(cert).build()
    }

    cm match {
      case Insecure     => none[ChannelCredentials].pure[F]
      case Secure(cert) => make(cert).map(_.some)
    }

  }

  implicit final class ManagedChannelBuilderOps[MCB <: ManagedChannelBuilder[MCB]](val mcb: MCB) extends AnyVal {

    def resource[F[_]: Sync](shutdownAwait: FiniteDuration): Resource[F, ManagedChannel] =
      resourceWithShutdown[F] { ch =>
        Sync[F].delay {
          ch.shutdown()
          if (!blocking(ch.awaitTermination(shutdownAwait.length, shutdownAwait.unit))) {
            ch.shutdownNow()
            ()
          }
        }
      }

    def resourceWithShutdown[F[_]: Sync](shutdown: ManagedChannel => F[Unit]): Resource[F, ManagedChannel] =
      Resource.make(Sync[F].delay(mcb.build()))(shutdown)

  }

}
