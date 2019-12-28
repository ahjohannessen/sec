package sec
package api
package mapping

import cats.implicits._

object helpers {

  private[sec] implicit final class AttemptOps[T](inner: Attempt[T]) {
    def unsafe: T = inner.leftMap(require(false, _)).toOption.get
  }

}
