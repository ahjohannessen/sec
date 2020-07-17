package sec
package cluster

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.Sync
import sec.api.Gossip._
import sec.api.Gossip.VNodeState._

object NodePrioritizer {

  final private[sec] val notAllowedStates: Set[VNodeState] =
    Set(
      Manager,
      ShuttingDown,
      Manager,
      Shutdown,
      Unknown,
      Initializing,
      CatchingUp,
      ResigningLeader,
      ShuttingDown,
      PreLeader,
      PreReplica,
      PreReadOnlyReplica,
      Clone,
      DiscoverLeader
    )

  private[sec] def pickBestNode[F[_]: Sync](
    members: NonEmptyList[MemberInfo],
    preference: NodePreference
  ): F[Option[MemberInfo]] = prioritizeNodes[F](members, preference).map(_.headOption)

  private[sec] def prioritizeNodes[F[_]: Sync](
    members: NonEmptyList[MemberInfo],
    preference: NodePreference
  ): F[List[MemberInfo]] =
    prioritizeNodes[F](members, preference, (m: MemberInfo) => !notAllowedStates.contains(m.state))

  private[sec] def prioritizeNodes[F[_]: Sync](
    members: NonEmptyList[MemberInfo],
    preference: NodePreference,
    allowed: MemberInfo => Boolean
  ): F[List[MemberInfo]] = {

    val candidates: List[MemberInfo] =
      members.filter(_.isAlive).filter(allowed).sortBy(_.state).reverse

    def arrange(p: MemberInfo => Boolean): F[List[MemberInfo]] = {
      val (satisfy, remaining) = candidates.partition(p)
      satisfy.shuffle[F].map(_ ::: remaining)
    }

    val isReadOnlyReplicaState: MemberInfo => Boolean =
      m => m.state === ReadOnlyLeaderless || m.state === ReadOnlyReplica

    preference match {
      case NodePreference.Leader          => arrange(_.state === Leader)
      case NodePreference.Follower        => arrange(_.state === Follower)
      case NodePreference.ReadOnlyReplica => arrange(isReadOnlyReplicaState)
    }
  }

}
