import cats.implicits._

package object sec {

//======================================================================================================================

  private[sec] type Attempt[T] = Either[String, T]

  private[sec] def guardNonEmpty(param: String): String => Attempt[String] =
    p => Either.fromOption(Option(p).filter(_.nonEmpty), s"$param cannot be empty")

  private[sec] def guardNotStartsWith(prefix: String): String => Attempt[String] =
    n => Either.cond(!n.startsWith(prefix), n, s"value must not start with $prefix, but is $n")

//======================================================================================================================

  private[sec] implicit final class BooleanOps(private val b: Boolean) extends AnyVal {
    def fold[A](t: => A, f: => A): A = if (b) t else f
  }

//======================================================================================================================

}
