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

import java.lang.{Long => JLong}
import cats.syntax.all._
import cats.kernel.laws.discipline._
import org.scalacheck._
import org.scalacheck.Prop.forAll
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
      assertEquals(StreamPosition(-1L), StreamPosition.Exact(ULong.MaxValue))
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
      assertEquals(LogPosition(-1L, -1L), Right(LogPosition.Exact.create(ULong.MaxValue, ULong.MaxValue)))
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

  group("PositionInfo") {

    test("renderPosition") {

      val stream = StreamPosition(1L)
      val all    = PositionInfo.Global(stream, LogPosition.exact(2L, 3L))

      assertEquals((stream: PositionInfo).renderPosition, "stream: 1")
      assertEquals((all: PositionInfo).renderPosition, "log: (c = 2, p = 3), stream: 1")
    }

    test("streamPosition") {

      val stream = StreamPosition(1L)
      val all    = PositionInfo.Global(stream, LogPosition.exact(2L, 3L))

      assertEquals((stream: PositionInfo).streamPosition, StreamPosition(1L))
      assertEquals((all: PositionInfo).streamPosition, StreamPosition(1L))

    }

  }

  group("ULong") {

    property("n >= 0") {
      forAll((n: ULong) => n >= ULong.MinValue)
    }

    property("a < b") {
      forAll((a: ULong, b: ULong) => a < b == a.toBigInt < b.toBigInt)
    }

    property("a <= b") {
      forAll((a: ULong, b: ULong) => a <= b == a.toBigInt <= b.toBigInt)
    }

    property("a > b") {
      forAll((a: ULong, b: ULong) => a > b == a.toBigInt > b.toBigInt)
    }

    property("a >= b") {
      forAll((a: ULong, b: ULong) => a >= b == a.toBigInt >= b.toBigInt)
    }

    test("MaxValue / MinValue") {
      assertEquals(ULong.MaxValue, ULong(-1))
      assertEquals(ULong.MinValue, ULong(0))
    }

    test("toLong") {
      assertEquals(ULong.MaxValue.toLong, -1L)
      assertEquals(ULong.MinValue.toLong, 0L)
    }

    test("toBigInt") {
      assertEquals(ULong.MaxValue.toBigInt, BigInt("18446744073709551615"))
      assertEquals(ULong.MinValue.toBigInt, BigInt(0))
    }

    property("n.toBigInt == BigInt(Long.toUnsignedString(n.toLong))") {
      forAll((n: ULong) => n.toBigInt == BigInt(JLong.toUnsignedString(n.toLong)))
    }

    property("n.render") {
      forAll((n: ULong) => n.render == n.toBigInt.toString)
    }

    property("n.toString = n.toBigInt.toString") {
      forAll((n: ULong) => n.toString == n.toBigInt.toString)
    }

    test("Order") {
      implicit val cogen: Cogen[ULong] = Cogen[BigInt].contramap[ULong](_.toBigInt)
      checkAll("ULong", OrderTests[ULong].order)
    }

  }

}
