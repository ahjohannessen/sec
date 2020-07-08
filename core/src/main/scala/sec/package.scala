import scala.util.Random
import cats.{ApplicativeError, MonadError}
import cats.implicits._
import cats.effect.Sync

package object sec {

//======================================================================================================================

  private[sec] type ErrorM[F[_]] = MonadError[F, Throwable]
  private[sec] type ErrorA[F[_]] = ApplicativeError[F, Throwable]
  private[sec] type Attempt[T]   = Either[String, T]

  type NodePreference = sec.cluster.NodePreference
  val NodePreference = sec.cluster.NodePreference

  type UserCredentials = sec.api.UserCredentials
  val UserCredentials = sec.api.UserCredentials

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

}
