/*
 * Copyright 2020 Scala EventStoreDB Client
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
import cats.ApplicativeThrow
import cats.syntax.all.*

private[sec] object time:

  /** @param value
    *   100-nanosecond intervals elapsed since 1970-01-01T00:00:00Z
    */
  def fromTicksSinceEpoch[F[_]: ApplicativeThrow](value: Long): F[ZonedDateTime] =

    val unitsPerSecond = 10000000L
    val seconds        = value / unitsPerSecond
    val nanos          = (value % unitsPerSecond) * 100

    Either
      .catchNonFatal(Instant.EPOCH.plusSeconds(seconds).plusNanos(nanos).atZone(ZoneOffset.UTC))
      .leftMap(DecodingError(_))
      .liftTo[F]
