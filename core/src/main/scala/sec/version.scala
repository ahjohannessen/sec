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

import cats.syntax.all._
import cats.{Eq, Order, Show}

//======================================================================================================================

sealed trait StreamState
object StreamState {

  case object NoStream     extends StreamState
  case object Any          extends StreamState
  case object StreamExists extends StreamState

  implicit val eq: Eq[StreamState] = Eq.fromUniversalEquals
  implicit val showForStreamState: Show[StreamState] = Show.show {
    case NoStream                => "NoStream"
    case Any                     => "Any"
    case StreamExists            => "StreamExists"
    case StreamPosition.Exact(v) => s"Exact(${v}L)"
  }

}

//======================================================================================================================

sealed trait StreamPosition
object StreamPosition {

  val Start: Exact = exact(0L)

  sealed abstract case class Exact(value: Long) extends StreamPosition with StreamState
  object Exact {

    private[StreamPosition] def create(value: Long): Exact = new Exact(value) {}

    def apply(value: Long): Either[InvalidInput, Exact] =
      Either.cond(value >= 0L, create(value), InvalidInput(s"value must be >= 0, but is $value"))
  }

  case object End extends StreamPosition

  ///

  private[sec] def exact(value: Long): Exact          = Exact.create(value)
  def apply(value: Long): Either[InvalidInput, Exact] = Exact(value)

  ///

  implicit val orderForStreamPosition: Order[StreamPosition] = Order.from {
    case (x: Exact, y: Exact) => Order[Exact].compare(x, y)
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

  implicit val orderForExact: Order[Exact] = Order.by(_.value)

  implicit val showForExact: Show[Exact] = Show.show[Exact](e => s"${e.value}L")

  implicit val showForStreamPosition: Show[StreamPosition] = Show.show[StreamPosition] {
    case e: Exact => e.show
    case End      => "end"
  }
}

//======================================================================================================================

sealed trait LogPosition
object LogPosition {

  val Start: Exact = exact(0L, 0L)

  sealed abstract case class Exact(commit: Long, prepare: Long) extends LogPosition
  object Exact {

    private[LogPosition] def create(commit: Long, prepare: Long): Exact =
      new Exact(commit, prepare) {}

    def apply(commit: Long, prepare: Long): Either[InvalidInput, Exact] = {

      val result = for {
        c <- Either.cond(commit >= 0, commit, s"commit must be >= 0, but is $commit")
        p <- Either.cond(prepare >= 0, prepare, s"prepare must be >= 0, but is $prepare")
        e <- Either.cond(commit >= prepare, create(c, p), s"commit must be >= prepare, but $commit < $prepare")
      } yield e

      result.leftMap(InvalidInput)
    }

  }

  case object End extends LogPosition

  ///

  private[sec] def exact(commit: Long, prepare: Long): Exact          = Exact.create(commit, prepare)
  def apply(commit: Long, prepare: Long): Either[InvalidInput, Exact] = Exact(commit, prepare)

  ///

  implicit val orderForLogPosition: Order[LogPosition] = Order.from {
    case (x: Exact, y: Exact) => Order[Exact].compare(x, y)
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

  implicit val orderForExact: Order[Exact] = Order.from { (x: Exact, y: Exact) =>
    (x.commit compare y.commit, x.prepare compare y.prepare) match {
      case (0, 0) => 0
      case (0, x) => x
      case (x, _) => x
    }
  }

  implicit val showForExact: Show[Exact] = Show.show[Exact] { case Exact(c, p) =>
    s"LogPosition(c = $c, p = $p)"
  }

  implicit val showForLogPosition: Show[LogPosition] = Show.show[LogPosition] {
    case e: Exact => e.show
    case End      => "end"
  }

}
