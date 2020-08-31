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

import java.net.InetSocketAddress
import io.grpc.{Attributes, EquivalentAddressGroup}
import cats.{Order, Show}
import cats.implicits._

final case class Endpoint(
  address: String,
  port: Int
)

object Endpoint {

  implicit val orderForEndpoint: Order[Endpoint] = Order.by(ep => (ep.address, ep.port))
  implicit val showForEndpoint: Show[Endpoint]   = Show.show(ep => s"${ep.address}:${ep.port}")

  implicit final class EndpointOps(val ep: Endpoint) extends AnyVal {
    def toInetSocketAddress: InetSocketAddress = new InetSocketAddress(ep.address, ep.port)
    def toEquivalentAddressGroup: EquivalentAddressGroup =
      new EquivalentAddressGroup(ep.toInetSocketAddress, Attributes.EMPTY)
  }
}
