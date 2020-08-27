package sec

import cats.effect._
import fs2.Stream

package object client {

  implicit final class SingleNodeBuilderOps[F[_]: ConcurrentEffect: Timer](val b: SingleNodeBuilder[F]) {
    def stream: Stream[F, EsClient[F]]     = Stream.resource[F, EsClient[F]](resource)
    def resource: Resource[F, EsClient[F]] = b.build(netty.mkBuilder[F])
  }

  implicit final class ClusterBuilderOps[F[_]: ConcurrentEffect: Timer](val b: ClusterBuilder[F]) {
    def stream: Stream[F, EsClient[F]]     = Stream.resource[F, EsClient[F]](resource)
    def resource: Resource[F, EsClient[F]] = b.build(netty.mkBuilder[F])
  }

}
