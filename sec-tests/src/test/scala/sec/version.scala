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
import cats.kernel.laws.discipline._
import org.scalacheck._
import org.specs2.mutable.Specification
import org.typelevel.discipline.specs2.mutable.Discipline
import sec.arbitraries._

class VersionSpec extends Specification with Discipline {

  "StreamRevision" >> {

    "Show" >> {

      def test(sr: StreamRevision, expected: String) =
        sr.show shouldEqual expected

      test(StreamRevision.NoStream, "NoStream")
      test(StreamRevision.Any, "Any")
      test(StreamRevision.StreamExists, "StreamExists")
      test(EventNumber.Start, "Exact(0)")
    }

    "Eq" >> {
      implicit val cogen: Cogen[StreamRevision] = Cogen[String].contramap[StreamRevision](_.show)
      checkAll("StreamRevision", EqTests[StreamRevision].eqv)
    }
  }

  "EventNumber" >> {

    import EventNumber.Exact
    import EventNumber.Exact.InvalidExact

    "apply" >> {
      EventNumber(-1L) shouldEqual EventNumber.End
      EventNumber(0L) shouldEqual EventNumber.exact(0L)
      EventNumber(1L) shouldEqual EventNumber.exact(1L)
    }

    "Exact.apply" >> {
      Exact(-1L) should beLeft("value must be >= 0, but is -1")
      Exact(0L) should beRight(EventNumber.exact(0L))
    }

    "Exact.lift" >> {
      Exact.lift[ErrorOr](-1L) should beLeft(InvalidExact("value must be >= 0, but is -1"))
      Exact.lift[ErrorOr](0L) should beRight(EventNumber.exact(0L))
    }

    "Show" >> {
      (EventNumber.Start: EventNumber).show shouldEqual "EventNumber(0)"
      (EventNumber.End: EventNumber).show shouldEqual "end"
    }

    "Order" >> {
      implicit val cogen: Cogen[EventNumber] = Cogen[String].contramap[EventNumber](_.show)
      checkAll("EventNumber", OrderTests[EventNumber].order)
    }
  }

  "Position" >> {

    import Position.Exact

    "apply" >> {
      Position(-1L, -1L) should beRight[Position](Position.End)
      Position(-1L, 0L) should beRight[Position](Position.End)
      Position(0L, -1L) should beRight[Position](Position.End)
      Position(0L, 0L) should beRight(Position.exact(0L, 0L))
      Position(1L, 0L) should beRight(Position.exact(1L, 0L))
      Position(1L, 1L) should beRight(Position.exact(1L, 1L))
      Position(0L, 1L) should beLeft("commit must be >= prepare, but 0 < 1")
    }

    "Exact.apply" >> {
      Exact(-1L, 0L) should beLeft("commit must be >= 0, but is -1")
      Exact(0L, -1L) should beLeft("prepare must be >= 0, but is -1")
      Exact(0L, 1L) should beLeft("commit must be >= prepare, but 0 < 1")
      Exact(0L, 0L) should beRight(Position.exact(0L, 0L))
      Exact(1L, 0L) should beRight(Position.exact(1L, 0L))
    }

    "Show" >> {
      (Position.Start: Position).show shouldEqual "Position(c = 0, p = 0)"
      (Position.End: Position).show shouldEqual "end"
    }

    "Order" >> {
      implicit val cogen: Cogen[Position] = Cogen[String].contramap[Position](_.show)
      checkAll("Position", OrderTests[Position].order)
    }
  }

}
