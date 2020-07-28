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
