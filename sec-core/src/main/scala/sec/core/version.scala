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
package core

import cats.{Eq, Order, Show}
import cats.syntax.all._

//======================================================================================================================

sealed trait StreamRevision
object StreamRevision {

  case object NoStream     extends StreamRevision
  case object Any          extends StreamRevision
  case object StreamExists extends StreamRevision

  implicit val eq: Eq[StreamRevision] = Eq.fromUniversalEquals
  implicit val showForStreamRevision: Show[StreamRevision] = Show.show {
    case NoStream             => "NoStream"
    case Any                  => "Any"
    case StreamExists         => "StreamExists"
    case EventNumber.Exact(v) => s"Exact($v)"
  }

}

//======================================================================================================================

sealed trait EventNumber
object EventNumber {

  val Start: Exact = exact(0L)

  sealed abstract case class Exact(value: Long) extends EventNumber with StreamRevision
  object Exact {
    private[EventNumber] def create(value: Long): Exact = new Exact(value) {}
    def apply(value: Long): Attempt[Exact] =
      Either.cond(value >= 0L, create(value), s"value must be >= 0, but is $value")
  }

  case object End extends EventNumber

  private[sec] def exact(value: Long): Exact = Exact.create(value)

  ///

  def apply(number: Long): EventNumber = if (number < 0) End else exact(number)

  implicit val orderForEventNumber: Order[EventNumber] = Order.from {
    case (x: Exact, y: Exact) => Order[Exact].compare(x, y)
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

  implicit val orderForExact: Order[Exact] = Order.by(_.value)

  implicit val showForExact: Show[Exact] = Show.show[Exact] { case Exact(v) =>
    s"EventNumber($v)"
  }

  implicit val showForEventNumber: Show[EventNumber] = Show.show[EventNumber] {
    case e: Exact => e.show
    case End      => "end"
  }
}

//======================================================================================================================

sealed trait Position
object Position {

  val Start: Exact = exact(0L, 0L)

  sealed abstract case class Exact(commit: Long, prepare: Long) extends Position
  object Exact {

    private[Position] def create(commit: Long, prepare: Long): Exact =
      new Exact(commit, prepare) {}

    def apply(commit: Long, prepare: Long): Attempt[Exact] =
      for {
        c <- Either.cond(commit >= 0, commit, s"commit must be >= 0, but is $commit")
        p <- Either.cond(prepare >= 0, prepare, s"prepare must be >= 0, but is $prepare")
        e <- Either.cond(commit >= prepare, create(c, p), s"commit must be >= prepare, but $commit < $prepare")
      } yield e

  }

  case object End extends Position

  private[sec] def exact(commit: Long, prepare: Long): Exact = Exact.create(commit, prepare)

  ///

  def apply(commit: Long, prepare: Long): Attempt[Position] =
    if (commit < 0 || prepare < 0) End.asRight else Exact(commit, prepare)

  implicit val orderForPosition: Order[Position] = Order.from {
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
    s"Position(c = $c, p = $p)"
  }

  implicit val showForEventNumber: Show[Position] = Show.show[Position] {
    case e: Exact => e.show
    case End      => "end"
  }

}
