package sec
package api

import scala.concurrent.duration._
import cats.effect.{Concurrent, Timer}
import io.chrisdavenport.log4cats.Logger

//======================================================================================================================

final private[sec] case class Opts[F[_]](
  oo: OperationOptions,
  retryOn: Throwable => Boolean,
  log: Logger[F]
)

private[sec] object Opts {

  implicit final class OptsOps[F[_]](val opts: Opts[F]) extends AnyVal {
    import opts._
    import opts.oo._

    def run[A](fa: F[A], opName: String)(implicit F: Concurrent[F], T: Timer[F]): F[A] = retryEnabled.fold(
      retry[F, A](fa, opName, retryDelay, retryStrategy.nextDelay, retryMaxAttempts, None, log)(retryOn),
      fa
    )
  }
}

//======================================================================================================================

final private[sec] case class OperationOptions(
  retryEnabled: Boolean,
  retryDelay: FiniteDuration,
  retryStrategy: RetryStrategy,
  retryMaxAttempts: Int
)

private[sec] object OperationOptions {
  val default: OperationOptions = OperationOptions(
    retryEnabled     = true,
    retryDelay       = 200.millis,
    retryStrategy    = RetryStrategy.Identity,
    retryMaxAttempts = 100
  )
}

//======================================================================================================================

sealed private[sec] trait RetryStrategy
private[sec] object RetryStrategy {

  case object Identity    extends RetryStrategy
  case object Exponential extends RetryStrategy

  implicit final class RetryStrategyOps(val s: RetryStrategy) extends AnyVal {
    def nextDelay(d: FiniteDuration): FiniteDuration = s match {
      case Identity    => d
      case Exponential => d * 2
    }
  }
}

//======================================================================================================================
