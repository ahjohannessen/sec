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

import cats.effect.*
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder.{forAddress, forTarget}

private[sec] object netty:

  def mkBuilder[F[_]: Sync](p: ChannelBuilderParams): F[NettyChannelBuilder] = Sync[F].delay {
    p.targetOrEndpoint.fold(
      t => p.creds.fold(forTarget(t).usePlaintext())(forTarget(t, _)),
      ep => p.creds.fold(forAddress(ep.address, ep.port).usePlaintext())(forAddress(ep.address, ep.port, _))
    )
  }
