package sec
package cluster
package grpc

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import cats.effect.implicits._
import io.grpc.NameResolver
import io.grpc.NameResolver.Listener2
import fs2.Stream
import sec.api.Gossip._

final private[sec] case class Resolver[F[_]: Effect](
  authority: String,
  notifier: Notifier[F]
) extends NameResolver {
  override def start(l: Listener2): Unit   = notifier.start(Listener[F](l)).toIO.unsafeRunSync()
  override val shutdown: Unit              = notifier.stop.toIO.unsafeRunSync()
  override val getServiceAuthority: String = authority
  override val refresh: Unit               = ()
}

private[sec] object Resolver {

  def gossip[F[_]: ConcurrentEffect](
    authority: String,
    np: NodePreference,
    seed: NonEmptyList[Endpoint],
    updates: Stream[F, ClusterInfo]
  ): F[Resolver[F]] =
    Notifier.gossip[F](seed, np, updates).map(Resolver[F](authority, _))

  def bestNodes[F[_]: ConcurrentEffect](
    authority: String,
    np: NodePreference,
    updates: Stream[F, ClusterInfo]
  ): F[Resolver[F]] =
    Notifier.bestNodes[F](np, updates).map(Resolver[F](authority, _))

}
