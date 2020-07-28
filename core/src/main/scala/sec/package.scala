import scala.concurrent.duration.FiniteDuration
import scala.util.Random
import scala.util.control.NonFatal
import cats.{ApplicativeError, MonadError}
import cats.implicits._
import cats.effect.{Concurrent, Sync, Timer}
import cats.effect.implicits._
import io.chrisdavenport.log4cats.Logger

package object sec {

//======================================================================================================================

  private[sec] type ErrorM[F[_]] = MonadError[F, Throwable]
  private[sec] type ErrorA[F[_]] = ApplicativeError[F, Throwable]
  private[sec] type Attempt[T]   = Either[String, T]

//======================================================================================================================

  private[sec] def guardNonEmpty(param: String): String => Attempt[String] =
    p => Either.fromOption(Option(p).filter(_.nonEmpty), s"$param cannot be empty")

  private[sec] def guardNotStartsWith(prefix: String): String => Attempt[String] =
    n => Either.cond(!n.startsWith(prefix), n, s"value must not start with $prefix, but is $n")

//======================================================================================================================

  implicit final private[sec] class BooleanOps(val b: Boolean) extends AnyVal {
    def fold[A](t: => A, f: => A): A = if (b) t else f
  }

//======================================================================================================================

  implicit final private[sec] class AttemptOps[A](val inner: Attempt[A]) extends AnyVal {
    def unsafe: A                                           = inner.leftMap(require(false, _)).toOption.get
    def orFail[F[_]: ErrorA](fn: String => Throwable): F[A] = inner.leftMap(fn(_)).liftTo[F]
  }

//======================================================================================================================

  implicit final private[sec] class ListOps[A](val inner: List[A]) extends AnyVal {
    def shuffle[F[_]: Sync]: F[List[A]] = Sync[F].delay(Random.shuffle(inner))
  }

//======================================================================================================================

  private[sec] def retryF[F[_]: Concurrent: Timer, A](
    fa: F[A],
    opName: String,
    delay: FiniteDuration,
    nextDelay: FiniteDuration => FiniteDuration,
    maxAttempts: Int,
    timeout: Option[FiniteDuration],
    log: Logger[F]
  )(retriable: Throwable => Boolean): F[A] = {

    def logWarn(attempt: Int, delay: FiniteDuration, error: Throwable): F[Unit] = log.warn(
      s"Failed $opName, attempt $attempt of $maxAttempts, retrying in $delay. Details: ${error.getMessage}"
    )

    def logError(error: Throwable): F[Unit] = log.error(
      s"Failed $opName after $maxAttempts attempts. Details: ${error.getMessage}"
    )

    val max          = math.max(maxAttempts, 1)
    val action: F[A] = timeout.fold(fa)(fa.timeout)

    def run(attempts: Int, d: FiniteDuration): F[A] = action.recoverWith {
      case NonFatal(t) if retriable(t) =>
        val attempt = attempts + 1
        if (attempt < max)
          logWarn(attempt, d, t) *> Timer[F].sleep(d) *> run(attempt, nextDelay(d))
        else
          logError(t) *> t.raiseError[F, A]
    }

    run(0, delay)
  }

}
