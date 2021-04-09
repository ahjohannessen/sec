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

import cats.syntax.all._
import cats.effect._
import com.typesafe.config.{Config, ConfigFactory}
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import sec.tsc.config.mkClient

trait EsClientObjectSyntax {

  implicit def esClientObjectSyntax(ec: EsClient.type): EsClientObjectOps = new EsClientObjectOps(ec)

  final class EsClientObjectOps(val ec: EsClient.type) {

    def fromConfig[F[_]: ConcurrentEffect: Timer]: Resource[F, EsClient[F]] =
      Resource.eval(Sync[F].delay(ConfigFactory.load)) >>= { cfg =>
        fromConfig[F](cfg, NoOpLogger[F])
      }

    def fromConfig[F[_]: ConcurrentEffect: Timer](logger: Logger[F]): Resource[F, EsClient[F]] =
      Resource.eval(Sync[F].delay(ConfigFactory.load)) >>= { cfg =>
        fromConfig[F](cfg, logger)
      }

    def fromConfig[F[_]: ConcurrentEffect: Timer](cfg: Config, logger: Logger[F]): Resource[F, EsClient[F]] =
      mkClient[F, NettyChannelBuilder](netty.mkBuilder[F], cfg, logger)
  }

}
