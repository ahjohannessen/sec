package sec
package api
package cluster
package grpc

import io.grpc.NameResolver.{Listener2, ResolutionResult}
import cats.effect.Sync

trait Listener[F[_]] {
  def onResult(result: ResolutionResult): F[Unit]
}

object Listener {
  def apply[F[_]](l2: Listener2)(implicit F: Sync[F]): Listener[F] =
    (rr: ResolutionResult) => F.delay(l2.onResult(rr))
}
