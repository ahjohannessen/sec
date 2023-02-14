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
package cluster

import cats.data.NonEmptySet
import com.comcast.ip4s.Hostname

sealed private[sec] trait ClusterEndpoints
private[sec] object ClusterEndpoints {

  /** Used when discovering endpoints via DNS.
    *
    * @param clusterDns
    *   DNS name to use for discovering endpoints.
    */
  final case class ViaDns(
    clusterDns: Hostname
  ) extends ClusterEndpoints

  /** Used when connecting via gossip seeds.
    *
    * @param endpoints
    *   Endpoints that provide cluster info.
    */
  final case class ViaSeed(
    endpoints: NonEmptySet[Endpoint]
  ) extends ClusterEndpoints

  //

  implicit final class ClusterEndpointsOps(val ce: ClusterEndpoints) {

    def fold[A](dns: ViaDns => A, seed: ViaSeed => A): A = ce match {
      case x: ViaDns  => dns(x)
      case x: ViaSeed => seed(x)
    }

  }

}
