package sec
package api
package mapping

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import cats.implicits._

private[sec] object time {

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

}
