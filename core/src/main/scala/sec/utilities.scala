/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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

import java.util.Locale
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import cats.syntax.all.*

private[sec] object utilities:

//======================================================================================================================

  def guardNonEmpty(param: String): String => Attempt[String] =
    p => Either.fromOption(Option(p).filter(_.nonEmpty), s"$param cannot be empty")

  def guardNotStartsWith(prefix: String): String => Attempt[String] =
    n => Either.cond(!n.startsWith(prefix), n, s"value must not start with $prefix, but is $n")

//======================================================================================================================

  def format(duration: Duration): String =

    def chooseUnit(fd: FiniteDuration): TimeUnit =
      if (fd.toDays > 0) TimeUnit.DAYS
      else if (fd.toHours > 0) TimeUnit.HOURS
      else if (fd.toMinutes > 0) TimeUnit.MINUTES
      else if (fd.toSeconds > 0) TimeUnit.SECONDS
      else if (fd.toMillis > 0) TimeUnit.MILLISECONDS
      else if (fd.toMicros > 0) TimeUnit.MICROSECONDS
      else TimeUnit.NANOSECONDS

    def abbreviate(tu: TimeUnit): String = tu match
      case TimeUnit.NANOSECONDS  => "ns"
      case TimeUnit.MICROSECONDS => "μs"
      case TimeUnit.MILLISECONDS => "ms"
      case TimeUnit.SECONDS      => "s"
      case TimeUnit.MINUTES      => "m"
      case TimeUnit.HOURS        => "h"
      case TimeUnit.DAYS         => "d"

    def format(d: FiniteDuration): String =

      val precision: Int = 4
      val nanos: Long    = d.toNanos
      val unit: TimeUnit = chooseUnit(d)
      val value: Double  = nanos.toDouble / NANOSECONDS.convert(1, unit)

      s"%.${precision}g%s".formatLocal(Locale.ROOT, value, abbreviate(unit))

    duration match
      case d: FiniteDuration => format(d)
      case d: Duration       => d.toString

//======================================================================================================================
