import cats.implicits._

package object sec {

//======================================================================================================================

  private[sec] type Attempt[T] = Either[String, T]

  private[sec] def guardNonEmpty(param: String): String => Attempt[String] =
    p => Either.fromOption(Option(p).filter(_.nonEmpty), s"$param cannot be empty")

//======================================================================================================================

  private[sec] implicit final class BooleanOps(private val b: Boolean) extends AnyVal {
    def fold[A](t: => A, f: => A): A = if (b) t else f
  }

//======================================================================================================================

}
