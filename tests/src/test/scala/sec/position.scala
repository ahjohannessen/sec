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

import cats.kernel.laws.discipline._
import org.scalacheck._
import org.specs2.mutable.Specification
import org.typelevel.discipline.specs2.mutable.Discipline
import sec.arbitraries._

class VersionSpec extends Specification with Discipline {

  "StreamState" >> {

    "render" >> {

      def test(ss: StreamState, expected: String) =
        ss.render shouldEqual expected

      test(StreamState.NoStream, "NoStream")
      test(StreamState.Any, "Any")
      test(StreamState.StreamExists, "StreamExists")
      test(StreamPosition.Start, "Exact(0L)")
    }

    "Eq" >> {
      implicit val cogen: Cogen[StreamState] = Cogen[String].contramap[StreamState](_.render)
      checkAll("StreamState", EqTests[StreamState].eqv)
    }
  }

  "StreamPosition" >> {

    "apply" >> {
      StreamPosition(0L) should beRight(StreamPosition.exact(0L))
      StreamPosition(1L) should beRight(StreamPosition.exact(1L))
      StreamPosition(-1L) should beLeft(InvalidInput("value must be >= 0, but is -1"))
    }

    "render" >> {
      (StreamPosition.Start: StreamPosition).render shouldEqual "0L"
      (StreamPosition.End: StreamPosition).render shouldEqual "end"
    }

    "Order" >> {
      implicit val cogen: Cogen[StreamPosition] = Cogen[String].contramap[StreamPosition](_.render)
      checkAll("StreamPosition", OrderTests[StreamPosition].order)
    }
  }

  "LogPosition" >> {

    "apply" >> {
      LogPosition(0L, 0L) should beRight(LogPosition.exact(0L, 0L))
      LogPosition(1L, 0L) should beRight(LogPosition.exact(1L, 0L))
      LogPosition(1L, 1L) should beRight(LogPosition.exact(1L, 1L))
      LogPosition(-1L, 0L) should beLeft(InvalidInput("commit must be >= 0, but is -1"))
      LogPosition(0L, -1L) should beLeft(InvalidInput("prepare must be >= 0, but is -1"))
      LogPosition(0L, 1L) should beLeft(InvalidInput("commit must be >= prepare, but 0 < 1"))
    }

    "render" >> {
      (LogPosition.Start: LogPosition).render shouldEqual "(c = 0L, p = 0L)"
      (LogPosition.End: LogPosition).render shouldEqual "end"
    }

    "Order" >> {
      implicit val cogen: Cogen[LogPosition] = Cogen[String].contramap[LogPosition](_.render)
      checkAll("LogPosition", OrderTests[LogPosition].order)
    }
  }

  "PositionInfo" >> {

    "renderPosition" >> {

      val stream = StreamPosition.exact(1L)
      val all    = PositionInfo.Global(stream, LogPosition.exact(2L, 3L))

      (stream: PositionInfo).renderPosition shouldEqual "stream: 1L"
      (all: PositionInfo).renderPosition shouldEqual "log: (c = 2L, p = 3L), stream: 1L"
    }

    "streamPosition" >> {

      val stream = StreamPosition.exact(1L)
      val all    = PositionInfo.Global(stream, LogPosition.exact(2L, 3L))

      (stream: PositionInfo).streamPosition shouldEqual StreamPosition.exact(1L)
      (all: PositionInfo).streamPosition shouldEqual StreamPosition.exact(1L)

    }

  }

}
