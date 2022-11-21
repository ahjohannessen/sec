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
import sec.arbitraries._

class VersionSuite extends SecDisciplineSuite {

  group("StreamState") {

    test("render") {

      def test(ss: StreamState, expected: String) =
        assertEquals(ss.render, expected)

      test(StreamState.NoStream, "NoStream")
      test(StreamState.Any, "Any")
      test(StreamState.StreamExists, "StreamExists")
      test(StreamPosition.Start, "Exact(0)")
    }

    test("Eq") {
      implicit val cogen: Cogen[StreamState] = Cogen[String].contramap[StreamState](_.render)
      checkAll("StreamState", EqTests[StreamState].eqv)
    }
  }

  group("StreamPosition") {

    test("apply") {
      assertEquals(StreamPosition(0L), StreamPosition.Exact(ULong(0L)))
      assertEquals(StreamPosition(1L), StreamPosition.Exact(ULong(1L)))
      assertEquals(StreamPosition(-1L), StreamPosition.Exact(ULong.max))
    }

    test("render") {
      assertEquals((StreamPosition.Start: StreamPosition).render, "0")
      assertEquals((StreamPosition.End: StreamPosition).render, "end")
    }

    test("Order") {
      implicit val cogen: Cogen[StreamPosition] = Cogen[String].contramap[StreamPosition](_.render)
      checkAll("StreamPosition", OrderTests[StreamPosition].order)
    }
  }

  group("LogPosition") {

    test("apply") {
      assertEquals(LogPosition(0L, 0L), Right(LogPosition.exact(0L, 0L)))
      assertEquals(LogPosition(1L, 0L), Right(LogPosition.exact(1L, 0L)))
      assertEquals(LogPosition(1L, 1L), Right(LogPosition.exact(1L, 1L)))
      assertEquals(LogPosition(-1L, -1L), Right(LogPosition.Exact.MaxValue))
      assertEquals(LogPosition(0L, 1L), Left(InvalidInput("commit must be >= prepare, but 0 < 1")))
    }

    test("render") {
      assertEquals((LogPosition.Start: LogPosition).render, "(c = 0, p = 0)")
      assertEquals((LogPosition.End: LogPosition).render, "end")
    }

    test("Order") {
      implicit val cogen: Cogen[LogPosition] = Cogen[String].contramap[LogPosition](_.render)
      checkAll("LogPosition", OrderTests[LogPosition].order)
    }
  }

}
