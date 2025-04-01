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

import java.util.UUID
import cats.syntax.all.*
import cats.effect.{Async, Resource, Sync}
import com.typesafe.config.{Config, ConfigFactory}
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import sec.tsc.config.mkClient
import sec.api.cluster.EndpointResolver

//======================================================================================================================

def mkUuid[F[_]: Sync]: F[UUID] = Sync[F].delay(UUID.randomUUID())

//======================================================================================================================

extension (ec: EsClient.type)

  def fromConfig[F[_]: Async]: Resource[F, EsClient[F]] =
    Resource.eval(Sync[F].delay(ConfigFactory.load)) >>= { cfg =>
      fromConfig[F](cfg, NoOpLogger[F])
    }

  def fromConfig[F[_]: Async](logger: Logger[F]): Resource[F, EsClient[F]] =
    Resource.eval(Sync[F].delay(ConfigFactory.load)) >>= { cfg =>
      fromConfig[F](cfg, logger)
    }

  def fromConfig[F[_]: Async](cfg: Config, logger: Logger[F]): Resource[F, EsClient[F]] =
    fromConfig[F](cfg, logger, EndpointResolver.default[F])

  private[sec] def fromConfig[F[_]: Async](
    cfg: Config,
    logger: Logger[F],
    er: EndpointResolver[F]
  ): Resource[F, EsClient[F]] =
    mkClient[F, NettyChannelBuilder](netty.mkBuilder[F], cfg, logger, er)

//======================================================================================================================

extension [F[_]: Async](b: SingleNodeBuilder[F]) def resource: Resource[F, EsClient[F]] = b.build(netty.mkBuilder[F])
extension [F[_]: Async](b: ClusterBuilder[F]) def resource: Resource[F, EsClient[F]]    = b.build(netty.mkBuilder[F])

//======================================================================================================================
