package sec
package api

import scala.concurrent.duration._
import cats.effect.{Concurrent, Timer}
import io.chrisdavenport.log4cats.Logger
import sec.api.retries._

//======================================================================================================================

final private[sec] case class Opts[F[_]](
  retryEnabled: Boolean,
  retryConfig: RetryConfig,
  retryOn: Throwable => Boolean,
  log: Logger[F]
)

private[sec] object Opts {

  implicit final class OptsOps[F[_]](val opts: Opts[F]) extends AnyVal {

    def run[A](fa: F[A], opName: String)(implicit F: Concurrent[F], T: Timer[F]): F[A] =
      opts.retryEnabled.fold(
        retry[F, A](fa, opName, opts.retryConfig, opts.log)(opts.retryOn),
        fa
      )

    def logWarn(opName: String)(attempt: Int, delay: FiniteDuration, error: Throwable): F[Unit] =
      opts.retryConfig.logWarn[F](opName, opts.log)(attempt, delay, error)

    def logError(opName: String)(error: Throwable): F[Unit] =
      opts.retryConfig.logError[F](opName, opts.log)(error)

  }
}

//======================================================================================================================

final private[sec] case class OperationOptions(
  retryEnabled: Boolean,
  retryDelay: FiniteDuration,
  retryMaxDelay: FiniteDuration,
  retryBackoffFactor: Double,
  retryMaxAttempts: Int
)

private[sec] object OperationOptions {

  val default: OperationOptions = OperationOptions(
    retryEnabled       = true,
    retryDelay         = 250.millis,
    retryMaxDelay      = 5.seconds,
    retryBackoffFactor = 1.5,
    retryMaxAttempts   = 100
  )

  implicit final class OperationOptionsOps(val oo: OperationOptions) extends AnyVal {

    def retryConfig: RetryConfig = RetryConfig(
      delay         = oo.retryDelay,
      maxDelay      = oo.retryMaxDelay,
      backoffFactor = oo.retryBackoffFactor,
      maxAttempts   = oo.retryMaxAttempts,
      timeout       = None
    )

  }

}
