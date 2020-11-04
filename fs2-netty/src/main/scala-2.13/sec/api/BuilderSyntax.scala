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

import cats.effect.{ConcurrentEffect, Resource, Timer}

//======================================================================================================================

trait BuilderSyntax {

  implicit def singleNodeBuilderSyntax[F[_]: ConcurrentEffect: Timer](
    snb: SingleNodeBuilder[F]
  ): SingleNodeBuilderOps[F] = new SingleNodeBuilderOps[F](snb)

  implicit def clusterBuilderSyntax[F[_]: ConcurrentEffect: Timer](
    cb: ClusterBuilder[F]
  ): ClusterBuilderOps[F] = new ClusterBuilderOps[F](cb)

}

//======================================================================================================================

final class SingleNodeBuilderOps[F[_]: ConcurrentEffect: Timer](val b: SingleNodeBuilder[F]) {
  def resource: Resource[F, EsClient[F]] = b.build(netty.mkBuilder[F])
}

final class ClusterBuilderOps[F[_]: ConcurrentEffect: Timer](val b: ClusterBuilder[F]) {
  def resource: Resource[F, EsClient[F]] = b.build(netty.mkBuilder[F])
}

//======================================================================================================================
