package sec
package cluster
package grpc

import io.grpc.LoadBalancer
import io.grpc.LoadBalancer.Helper
import io.grpc.LoadBalancerProvider

object GossipLbProvider extends LoadBalancerProvider {
  final val isAvailable: Boolean                          = true
  final val getPriority: Int                              = 5
  final val getPolicyName: String                         = "gossip"
  final def newLoadBalancer(helper: Helper): LoadBalancer = new GossipLb(helper)
}
