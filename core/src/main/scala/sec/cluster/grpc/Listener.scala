package sec
package cluster
package grpc

import io.grpc.Status
import io.grpc.NameResolver.{Listener2, ResolutionResult}
import cats.effect.Sync

trait Listener[F[_]] {
  def onResult(result: ResolutionResult): F[Unit]
  def onError(error: Status): F[Unit]
}

object Listener {
  def apply[F[_]](l2: Listener2)(implicit F: Sync[F]): Listener[F] =
    new Listener[F] {
      def onResult(result: ResolutionResult): F[Unit] = F.delay(l2.onResult(result))
      def onError(error: Status): F[Unit]             = F.delay(l2.onError(error))
    }
}
