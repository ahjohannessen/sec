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
      test(StreamPosition.Start, "0L")
    }

    "Eq" >> {
      implicit val cogen: Cogen[StreamRevision] = Cogen[String].contramap[StreamRevision](_.show)
      checkAll("StreamRevision", EqTests[StreamRevision].eqv)
    }
  }

  "StreamPosition" >> {

    import StreamPosition.Exact
    import StreamPosition.Exact.InvalidExact

    "apply" >> {
      StreamPosition(-1L) shouldEqual StreamPosition.End
      StreamPosition(0L) shouldEqual StreamPosition.exact(0L)
      StreamPosition(1L) shouldEqual StreamPosition.exact(1L)
    }

    "Exact.apply" >> {
      Exact(-1L) should beLeft("value must be >= 0, but is -1")
      Exact(0L) should beRight(StreamPosition.exact(0L))
    }

    "Exact.of" >> {
      Exact.of[ErrorOr](-1L) should beLeft(InvalidExact("value must be >= 0, but is -1"))
      Exact.of[ErrorOr](0L) should beRight(StreamPosition.exact(0L))
    }

    "Show" >> {
      (StreamPosition.Start: StreamPosition).show shouldEqual "0L"
      (StreamPosition.End: StreamPosition).show shouldEqual "end"
    }

    "Order" >> {
      implicit val cogen: Cogen[StreamPosition] = Cogen[String].contramap[StreamPosition](_.show)
      checkAll("StreamPosition", OrderTests[StreamPosition].order)
    }
  }

  "Position" >> {

    import LogPosition.Exact

    "apply" >> {
      LogPosition(-1L, -1L) should beRight[LogPosition](LogPosition.End)
      LogPosition(-1L, 0L) should beRight[LogPosition](LogPosition.End)
      LogPosition(0L, -1L) should beRight[LogPosition](LogPosition.End)
      LogPosition(0L, 0L) should beRight(LogPosition.exact(0L, 0L))
      LogPosition(1L, 0L) should beRight(LogPosition.exact(1L, 0L))
      LogPosition(1L, 1L) should beRight(LogPosition.exact(1L, 1L))
      LogPosition(0L, 1L) should beLeft("commit must be >= prepare, but 0 < 1")
    }

    "Exact.apply" >> {
      Exact(-1L, 0L) should beLeft("commit must be >= 0, but is -1")
      Exact(0L, -1L) should beLeft("prepare must be >= 0, but is -1")
      Exact(0L, 1L) should beLeft("commit must be >= prepare, but 0 < 1")
      Exact(0L, 0L) should beRight(LogPosition.exact(0L, 0L))
      Exact(1L, 0L) should beRight(LogPosition.exact(1L, 0L))
    }

    "Show" >> {
      (LogPosition.Start: LogPosition).show shouldEqual "LogPosition(c = 0, p = 0)"
      (LogPosition.End: LogPosition).show shouldEqual "end"
    }

    "Order" >> {
      implicit val cogen: Cogen[LogPosition] = Cogen[String].contramap[LogPosition](_.show)
      checkAll("LogPosition", OrderTests[LogPosition].order)
    }
  }

}
