package sec

import cats.effect._

package object client {

  implicit final class SingleNodeBuilderOps[F[_]: ConcurrentEffect: Timer](val b: SingleNodeBuilder[F]) {
    def resource: Resource[F, EsClient[F]] = b.build(netty.mkBuilder[F])
  }

  implicit final class ClusterBuilderOps[F[_]: ConcurrentEffect: Timer](val b: ClusterBuilder[F]) {
    def resource: Resource[F, EsClient[F]] = b.build(netty.mkBuilder[F])
  }

}
