package sec
package cluster
package grpc

import java.net.URI
import io.grpc._
import io.grpc.NameResolver._
import cats.data.NonEmptyList
import cats.effect._
import cats.effect.implicits._
import fs2.Stream
import sec.api.Gossip._

final private[sec] case class ResolverProvider[F[_]: Effect](
  authority: String,
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

  final val gossipScheme: String  = "gossip"
  final val clusterScheme: String = "cluster"

  def gossip[F[_]: ConcurrentEffect](
    authority: String,
    seed: NonEmptyList[Endpoint],
    np: NodePreference,
    updates: Stream[F, ClusterInfo]
  ): ResolverProvider[F] =
    ResolverProvider(authority, gossipScheme, Resolver.gossip(authority, np, seed, updates))

  def bestNodes[F[_]: ConcurrentEffect](
    authority: String,
    np: NodePreference,
    updates: Stream[F, ClusterInfo]
  ): ResolverProvider[F] =
    ResolverProvider(authority, clusterScheme, Resolver.bestNodes(authority, np, updates))

}
