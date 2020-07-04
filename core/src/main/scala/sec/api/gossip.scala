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
import sec.syntax.GossipSyntax
import mapping.gossip.mkClusterInfo

trait Gossip[F[_]] {
  def read(creds: Option[UserCredentials]): F[Gossip.ClusterInfo]
}

object Gossip {

  implicit def syntaxForGossip[F[_]](g: Gossip[F]): GossipSyntax[F] = new GossipSyntax[F](g)

  private[sec] def apply[F[_]: Sync](
    client: GossipFs2Grpc[F, Context],
    options: Options
  ): Gossip[F] = new Impl[F](client, options)

  final private[sec] class Impl[F[_]: Sync](
    val client: GossipFs2Grpc[F, Context],
    val options: Options
  ) extends Gossip[F] {

    val ctx: Option[UserCredentials] => Context = uc =>
      Context(options.connectionName, uc.orElse(options.defaultCreds), requiresLeader = false)

    def read(creds: Option[UserCredentials]): F[ClusterInfo] =
      client.read(Empty(), ctx(creds)) >>= mkClusterInfo[F]

  }

//======================================================================================================================

  final case class ClusterInfo(
    members: Set[MemberInfo]
  )

  object ClusterInfo {
    implicit val showForClusterInfo: Show[ClusterInfo] = Show.show { ci =>
      if (ci.members.nonEmpty) {
        s"""cluster-info:
         | ${ci.members.map(_.show).mkString("- ", "\n - ", "")}
         |""".stripMargin
      } else "cluster-info: no members"
    }
  }

  final case class MemberInfo(
    instanceId: ju.UUID,
    timestamp: ZonedDateTime,
    state: VNodeState,
    isAlive: Boolean,
    httpEndpoint: Endpoint
  )

  object MemberInfo {

    implicit val eqForMemberInfo: Eq[MemberInfo] = Eq.instance { (l, r) =>
      l.httpEndpoint === r.httpEndpoint &&
      l.state === r.state &&
      l.isAlive === r.isAlive &&
      l.instanceId === r.instanceId
    }

    implicit val showForMemberInfo: Show[MemberInfo] = Show.show { mi =>
      s"${mi.state}[alive=${mi.isAlive}, addr=${mi.httpEndpoint.address}:${mi.httpEndpoint.port}, ts=${mi.timestamp}, id=${mi.instanceId}]"
    }
  }

  final case class Endpoint(
    address: String,
    port: Int
  )

  object Endpoint {

    implicit val eqForEndpoint: Eq[Endpoint] = Eq.fromUniversalEquals[Endpoint]

    implicit final class EndpointOps(val ep: Endpoint) extends AnyVal {
      def toInetSocketAddress: InetSocketAddress           = new InetSocketAddress(ep.address, ep.port)
      def toEquivalentAddressGroup: EquivalentAddressGroup = new EquivalentAddressGroup(ep.toInetSocketAddress)
      def toEquivalentAddressGroup(attr: Attributes): EquivalentAddressGroup =
        new EquivalentAddressGroup(ep.toInetSocketAddress, attr)
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

    implicit val orderForNodeState: Order[VNodeState] = Order.by(_.id)

  }

}
