package sec
package api

import java.util.concurrent.TimeoutException
import scala.util.control.NonFatal
import scala.concurrent.duration.{Duration, FiniteDuration}
import cats.implicits._
import cats.effect.{Concurrent, Timer}
import cats.effect.implicits._
import io.chrisdavenport.log4cats.Logger

private[sec] object retries {

//======================================================================================================================

  sealed abstract case class RetryConfig(
    delay: FiniteDuration,
    maxDelay: FiniteDuration,
    backoffFactor: Double,
    maxAttempts: Int,
    timeout: Option[FiniteDuration]
  )

  object RetryConfig {

    def apply(
      delay: FiniteDuration,
      maxDelay: FiniteDuration,
      backoffFactor: Double,
      maxAttempts: Int,
      timeout: Option[FiniteDuration]
    ): RetryConfig =
      new RetryConfig(delay.min(maxDelay), maxDelay, backoffFactor.max(1), math.max(maxAttempts, 1), timeout) {}

    implicit final class RetryConfigOps(val c: RetryConfig) extends AnyVal {

      def nextDelay(d: FiniteDuration): FiniteDuration =
        (d * c.backoffFactor).min(c.maxDelay) match {
          case f: FiniteDuration    => f
          case _: Duration.Infinite => c.maxDelay
        }

      def logWarn[F[_]](action: String, log: Logger[F])(attempt: Int, delay: FiniteDuration, th: Throwable): F[Unit] =
        log.warn(
          s"$action failed, attempt $attempt of ${c.maxAttempts}, retrying in ${format(delay)} - ${th.getMessage}"
        )

      def logError[F[_]](action: String, log: Logger[F])(th: Throwable): F[Unit] =
        log.error(s"$action failed after ${c.maxAttempts} attempts - ${th.getMessage}")

    }
  }

//======================================================================================================================

  final case class Timeout(after: FiniteDuration) extends RuntimeException(s"Timed out after ${format(after)}.")

//======================================================================================================================

  // TODO: Tests

  def retry[F[_]: Concurrent: Timer, A](
    action: F[A],
    actionName: String,
    retryConfig: RetryConfig,
    log: Logger[F]
  )(retryOn: Throwable => Boolean): F[A] = {
    import retryConfig._

    def withTimeout(to: FiniteDuration): F[A] =
      action.timeout(to).adaptError { case _: TimeoutException => Timeout(to) }

    val fa       = timeout.fold(action)(withTimeout)
    val logWarn  = retryConfig.logWarn[F](actionName, log) _
    val logError = retryConfig.logError[F](actionName, log) _

    def run(attempts: Int, d: FiniteDuration): F[A] = fa.recoverWith {
      case NonFatal(t) if retryOn(t) =>
        val attempt = attempts + 1
        if (attempt < retryConfig.maxAttempts)
          logWarn(attempt, d, t) *> Timer[F].sleep(d) *> run(attempt, retryConfig.nextDelay(d))
        else
          logError(t) *> t.raiseError[F, A]
    }

    run(0, retryConfig.delay)
  }

//======================================================================================================================

}
