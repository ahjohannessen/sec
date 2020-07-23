package sec
package api

import java.net.InetSocketAddress
import io.grpc.{Attributes, EquivalentAddressGroup}
import java.{util => ju}
import java.time.ZonedDateTime
import cats._
import cats.implicits._
import cats.effect.Sync
import com.eventstore.client.gossip.GossipFs2Grpc
import com.eventstore.client.Empty
import mapping.gossip.mkClusterInfo

trait Gossip[F[_]] {
  def read(creds: Option[UserCredentials]): F[Gossip.ClusterInfo]
}

object Gossip {

  private[sec] def apply[F[_]: Sync](
    client: GossipFs2Grpc[F, Context],
    creds: Option[UserCredentials],
    connectionName: String
  ): Gossip[F] = new Impl[F](client, creds, connectionName)

  final private[sec] class Impl[F[_]: Sync](
    val client: GossipFs2Grpc[F, Context],
    creds: Option[UserCredentials],
    connectionName: String
  ) extends Gossip[F] {

    val ctx: Option[UserCredentials] => Context = uc =>
      Context(s"$connectionName-gossip", uc.orElse(creds), requiresLeader = false)

    def read(creds: Option[UserCredentials]): F[ClusterInfo] =
      client.read(Empty(), ctx(creds)) >>= mkClusterInfo[F]

  }

//======================================================================================================================

  final case class ClusterInfo(
    members: Set[MemberInfo]
  )

  object ClusterInfo {
    implicit val orderForClusterInfo: Order[ClusterInfo] = Order.by(_.members.toList.sorted)
  }

  final case class MemberInfo(
    instanceId: ju.UUID,
    timestamp: ZonedDateTime,
    state: VNodeState,
    isAlive: Boolean,
    httpEndpoint: Endpoint
  )

  object MemberInfo {
    implicit val orderForMemberInfo: Order[MemberInfo] =
      Order.by(mi => (mi.httpEndpoint, mi.state, mi.isAlive, mi.instanceId))
  }

  final case class Endpoint(
    address: String,
    port: Int
  )

  object Endpoint {

    implicit val orderForEndpoint: Order[Endpoint] = Order.by(ep => (ep.address, ep.port))

    implicit final class EndpointOps(val ep: Endpoint) extends AnyVal {
      def toInetSocketAddress: InetSocketAddress = new InetSocketAddress(ep.address, ep.port)
      def toEquivalentAddressGroup: EquivalentAddressGroup =
        new EquivalentAddressGroup(ep.toInetSocketAddress, Attributes.EMPTY)
    }
  }

  sealed abstract class VNodeState(
    val id: Int
  )

  object VNodeState {

    case object Initializing       extends VNodeState(0)
    case object DiscoverLeader     extends VNodeState(1)
    case object Unknown            extends VNodeState(2)
    case object PreReplica         extends VNodeState(3)
    case object CatchingUp         extends VNodeState(4)
    case object Clone              extends VNodeState(5)
    case object Follower           extends VNodeState(6)
    case object PreLeader          extends VNodeState(7)
    case object Leader             extends VNodeState(8)
    case object Manager            extends VNodeState(9)
    case object ShuttingDown       extends VNodeState(10)
    case object Shutdown           extends VNodeState(11)
    case object ReadOnlyLeaderless extends VNodeState(12)
    case object PreReadOnlyReplica extends VNodeState(13)
    case object ReadOnlyReplica    extends VNodeState(14)
    case object ResigningLeader    extends VNodeState(15)

    final private[sec] val values: List[VNodeState] = List(
      Initializing,
      DiscoverLeader,
      Unknown,
      PreReplica,
      CatchingUp,
      Clone,
      Follower,
      PreLeader,
      Leader,
      Manager,
      ShuttingDown,
      Shutdown,
      ReadOnlyLeaderless,
      PreReadOnlyReplica,
      ReadOnlyReplica,
      ResigningLeader
    )

    implicit val orderForVNodeState: Order[VNodeState] = Order.by(_.id)
  }

}
