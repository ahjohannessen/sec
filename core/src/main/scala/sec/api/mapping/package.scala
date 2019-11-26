package sec
package api

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import cats.implicits._
import scodec.bits.ByteVector
import com.google.protobuf.ByteString

package object mapping {

  implicit final class ByteVectorOps(val bv: ByteVector) extends AnyVal {
    def toByteString: ByteString = ByteString.copyFrom(bv.toByteBuffer)
  }

  implicit final class ByteStringOps(val bs: ByteString) extends AnyVal {
    def toByteVector: ByteVector = ByteVector.view(bs.asReadOnlyByteBuffer())
  }

  /**
   * @param value 100-nanosecond intervals elapsed since 1970-01-01T00:00:00Z
   * */
  def fromTicksSinceEpoch[F[_]: ErrorA](value: Long): F[ZonedDateTime] = {

    val unitsPerSecond = 10000000L
    val seconds        = value / unitsPerSecond
    val nanos          = (value % unitsPerSecond) * 100

    Either
      .catchNonFatal(Instant.EPOCH.plusSeconds(seconds).plusNanos(nanos).atZone(ZoneOffset.UTC))
      .leftMap(DecodingError(_))
      .liftTo[F]
  }

  ///

  implicit final class OptionOps[A](private val o: Option[A]) extends AnyVal {
    def require[F[_]: ErrorA](value: String): F[A] =
      o.toRight(ProtoResultError(s"Required value $value missing or invalid.")).liftTo[F]
  }

  // TODO: Break up into different types

  final case class EncodingError(msg: String) extends RuntimeException(msg)

  object EncodingError {
    def apply(e: Throwable): EncodingError = EncodingError(e.getMessage)
  }

  final case class DecodingError(msg: String) extends RuntimeException(msg)

  object DecodingError {
    def apply(e: Throwable): DecodingError = DecodingError(e.getMessage)
  }

  final case class ProtoResultError(msg: String) extends RuntimeException(msg)

}
