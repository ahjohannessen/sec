package sec
package grpc

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

  private final val dotnetEpoch = Instant.parse("0001-01-01T00:00:00Z")

  /**
   * TODO: Temporary workaround for dotnet specific encoding.
   * Not something that I wish to do as it is best
   * effort, probably full of edge cases and wtfs.
   *
   * @param value 100-nanosecond intervals elapsed since 0001-01-01T00:00:00Z
   * */
  def fromDateTimeBinaryUTC[F[_]: ErrorA](value: Long): F[ZonedDateTime] = {

    val unitsPerSecond = 10000000L
    val seconds        = value / unitsPerSecond
    val nanos          = (value % unitsPerSecond) * 100

    Either
      .catchNonFatal(dotnetEpoch.plusSeconds(seconds).plusNanos(nanos).atZone(ZoneOffset.UTC))
      .leftMap(e => ProtoResultError(e.getMessage))
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
    def apply(e: Exception): EncodingError = EncodingError(e.getMessage)
  }

  final case class DecodingError(msg: String) extends RuntimeException(msg)

  object DecodingError {
    def apply(e: Exception): DecodingError = DecodingError(e.getMessage)
  }

  final case class ProtoResultError(msg: String) extends RuntimeException(msg)

}
