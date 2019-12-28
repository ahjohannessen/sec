package sec
package api
package mapping

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import cats.implicits._
import org.specs2._
import sec.api.mapping.time.fromTicksSinceEpoch
import sec.api.mapping.implicits._

class TimeSpec extends mutable.Specification {

  "fromTicksSinceEpoch" >> {
    fromTicksSinceEpoch[Either[Throwable, *]](15775512069940048L) shouldEqual
      ZonedDateTime.parse("2019-12-28T16:40:06.994004800Z").asRight

    fromTicksSinceEpoch[Either[Throwable, *]](0L) shouldEqual
      Instant.EPOCH.atZone(ZoneOffset.UTC).asRight
  }

}
