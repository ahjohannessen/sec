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

import java.util.Locale
import scala.concurrent.duration._
import cats.syntax.all._

private[sec] object utilities {

//======================================================================================================================

  def guardNonEmpty(param: String): String => Attempt[String] =
    p => Either.fromOption(Option(p).filter(_.nonEmpty), s"$param cannot be empty")

  def guardNotStartsWith(prefix: String): String => Attempt[String] =
    n => Either.cond(!n.startsWith(prefix), n, s"value must not start with $prefix, but is $n")

//======================================================================================================================

  final class BooleanOps(val b: Boolean) extends AnyVal {
    def fold[A](t: => A, f: => A): A = if (b) t else f
  }

//======================================================================================================================

  final class AttemptOps[A](val inner: Attempt[A]) extends AnyVal {
    def unsafe: A                                           = inner.leftMap(require(false, _)).toOption.get
    def orFail[F[_]: ErrorA](fn: String => Throwable): F[A] = inner.leftMap(fn(_)).liftTo[F]
  }

//======================================================================================================================

  def format(duration: Duration): String = {

    def chooseUnit(fd: FiniteDuration): TimeUnit = {
      if (fd.toDays > 0) DAYS
      else if (fd.toHours > 0) HOURS
      else if (fd.toMinutes > 0) MINUTES
      else if (fd.toSeconds > 0) SECONDS
      else if (fd.toMillis > 0) MILLISECONDS
      else if (fd.toMicros > 0) MICROSECONDS
      else NANOSECONDS
    }

    def abbreviate(unit: TimeUnit): String = unit match {
      case NANOSECONDS  => "ns"
      case MICROSECONDS => "μs"
      case MILLISECONDS => "ms"
      case SECONDS      => "s"
      case MINUTES      => "m"
      case HOURS        => "h"
      case DAYS         => "d"
    }

    def format(d: FiniteDuration): String = {

      val precision: Int = 4
      val nanos: Long    = d.toNanos
      val unit: TimeUnit = chooseUnit(d)
      val value: Double  = nanos.toDouble / NANOSECONDS.convert(1, unit)

      s"%.${precision}g%s".formatLocal(Locale.ROOT, value, abbreviate(unit))
    }

    duration match {
      case d: FiniteDuration => format(d)
      case d: Duration       => d.toString
    }

  }

//======================================================================================================================

}
