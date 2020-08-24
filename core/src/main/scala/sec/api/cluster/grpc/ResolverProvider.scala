package sec
package api
package cluster
package grpc

import java.net.URI
import io.grpc._
import io.grpc.NameResolver._
import cats.data.NonEmptySet
import cats.implicits._
import cats.effect._
import cats.effect.implicits._
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.chrisdavenport.log4cats.Logger
import sec.api.Gossip._

final private[sec] case class ResolverProvider[F[_]: Effect](
  scheme: String,
  resolver: F[Resolver[F]]
) extends NameResolverProvider {
  override val getDefaultScheme: String = scheme
  override val isAvailable: Boolean     = true
  override val priority: Int            = 4 // Less important than DNS

  override def newNameResolver(uri: URI, args: Args): NameResolver =
    if (scheme == uri.getScheme) resolver.toIO.unsafeRunSync() else null
}

object ResolverProvider {

  final val gossipScheme: String  = "eventstore-gossip"
  final val clusterScheme: String = "eventstore-cluster"

  private def mkHaltR[F[_]: Concurrent](log: Logger[F]): Resource[F, SignallingRef[F, Boolean]] =
    Resource.make(SignallingRef[F, Boolean](false)) { sr =>
      sr.set(true) *> log.debug("signalled notifier to halt.")
    }

  def gossip[F[_]: ConcurrentEffect](
    authority: String,
    seed: NonEmptySet[Endpoint],
    updates: Stream[F, ClusterInfo],
    log: Logger[F]
  ): Resource[F, ResolverProvider[F]] =
    mkHaltR[F](log.withModifiedString(s => s"Gossip resolver: $s")).map { halt =>
      ResolverProvider(gossipScheme, Resolver.gossip(authority, seed, updates, halt))
    }

  def bestNodes[F[_]: ConcurrentEffect](
    authority: String,
    np: NodePreference,
    updates: Stream[F, ClusterInfo],
    log: Logger[F]
  ): Resource[F, ResolverProvider[F]] =
    mkHaltR[F](log.withModifiedString(s => s"BestNodes resolver: $s")).map { halt =>
      ResolverProvider(clusterScheme, Resolver.bestNodes(authority, np, updates, halt))
    }

}
