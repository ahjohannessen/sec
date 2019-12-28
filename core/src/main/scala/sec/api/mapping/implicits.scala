package sec
package api
package mapping

import cats.implicits._
import scodec.bits.ByteVector
import com.google.protobuf.ByteString

private[sec] object implicits {

  implicit final class ByteVectorOps(val bv: ByteVector) extends AnyVal {
    def toByteString: ByteString = ByteString.copyFrom(bv.toByteBuffer)
  }

  implicit final class ByteStringOps(val bs: ByteString) extends AnyVal {
    def toByteVector: ByteVector = ByteVector.view(bs.asReadOnlyByteBuffer())
  }

  ///

  implicit final class OptionOps[A](private val o: Option[A]) extends AnyVal {
    def require[F[_]: ErrorA](value: String): F[A] =
      o.toRight(ProtoResultError(s"Required value $value missing or invalid.")).liftTo[F]
  }

  implicit final class AttemptOps[T](inner: Attempt[T]) {
    def unsafe: T = inner.leftMap(require(false, _)).toOption.get
  }

}
