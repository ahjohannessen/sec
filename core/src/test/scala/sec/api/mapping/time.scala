/*
 * Copyright 2020 Alex Henning Johannessen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sec
package api
package mapping

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import cats.implicits._
import org.specs2._
import sec.api.mapping.time.fromTicksSinceEpoch

class TimeSpec extends mutable.Specification {

  "fromTicksSinceEpoch" >> {
    fromTicksSinceEpoch[ErrorOr](15775512069940048L) shouldEqual
      ZonedDateTime.parse("2019-12-28T16:40:06.994004800Z").asRight

    fromTicksSinceEpoch[ErrorOr](0L) shouldEqual
      Instant.EPOCH.atZone(ZoneOffset.UTC).asRight
  }

}
