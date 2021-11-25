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
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.typelevel.discipline.specs2.mutable.Discipline
import sec.arbitraries._

class VersionSpec extends Specification with Discipline with ScalaCheck {

  "StreamState" >> {

    "render" >> {

      def test(ss: StreamState, expected: String) =
        ss.render shouldEqual expected

      test(StreamState.NoStream, "NoStream")
      test(StreamState.Any, "Any")
      test(StreamState.StreamExists, "StreamExists")
      test(StreamPosition.Start, "Exact(0)")
    }

    "Eq" >> {
      implicit val cogen: Cogen[StreamState] = Cogen[String].contramap[StreamState](_.render)
      checkAll("StreamState", EqTests[StreamState].eqv)
    }
  }

  "StreamPosition" >> {

    "apply" >> {
      StreamPosition(0L) shouldEqual StreamPosition.Exact(ULong(0L))
      StreamPosition(1L) shouldEqual StreamPosition.Exact(ULong(1L))
      StreamPosition(-1L) shouldEqual StreamPosition.Exact(ULong.MaxValue)
    }

    "render" >> {
      (StreamPosition.Start: StreamPosition).render shouldEqual "0"
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
      LogPosition(-1L, -1L) should beRight(LogPosition.Exact.create(ULong.MaxValue, ULong.MaxValue))
      LogPosition(0L, 1L) should beLeft(InvalidInput("commit must be >= prepare, but 0 < 1"))
    }

    "render" >> {
      (LogPosition.Start: LogPosition).render shouldEqual "(c = 0, p = 0)"
      (LogPosition.End: LogPosition).render shouldEqual "end"
    }

    "Order" >> {
      implicit val cogen: Cogen[LogPosition] = Cogen[String].contramap[LogPosition](_.render)
      checkAll("LogPosition", OrderTests[LogPosition].order)
    }
  }

  "PositionInfo" >> {

    "renderPosition" >> {

      val stream = StreamPosition(1L)
      val all    = PositionInfo.Global(stream, LogPosition.exact(2L, 3L))

      (stream: PositionInfo).renderPosition shouldEqual "stream: 1"
      (all: PositionInfo).renderPosition shouldEqual "log: (c = 2, p = 3), stream: 1"
    }

    "streamPosition" >> {

      val stream = StreamPosition(1L)
      val all    = PositionInfo.Global(stream, LogPosition.exact(2L, 3L))

      (stream: PositionInfo).streamPosition shouldEqual StreamPosition(1L)
      (all: PositionInfo).streamPosition shouldEqual StreamPosition(1L)

    }

  }

  "ULong" >> {

    "n >= 0" >> {
      prop((n: ULong) => n >= ULong.MinValue == true)
    }

    "a < b" >> {
      prop((a: ULong, b: ULong) => a < b == a.toBigInt < b.toBigInt)
    }

    "a <= b" >> {
      prop((a: ULong, b: ULong) => a <= b == a.toBigInt <= b.toBigInt)
    }

    "a > b" >> {
      prop((a: ULong, b: ULong) => a > b == a.toBigInt > b.toBigInt)
    }

    "a >= b" >> {
      prop((a: ULong, b: ULong) => a >= b == a.toBigInt >= b.toBigInt)
    }

    "MaxValue / MinValue" >> {
      ULong.MaxValue shouldEqual ULong(-1)
      ULong.MinValue shouldEqual ULong(0)
    }

    "toLong" >> {
      ULong.MaxValue.toLong shouldEqual -1
      ULong.MinValue.toLong shouldEqual 0
    }

    "toBigInt" >> {
      ULong.MaxValue.toBigInt shouldEqual BigInt("18446744073709551615")
      ULong.MinValue.toBigInt shouldEqual BigInt(0)
    }

    "n.toBigInt == BigInt(Long.toUnsignedString(n.toLong))" >> {
      prop((n: ULong) => n.toBigInt == BigInt(JLong.toUnsignedString(n.toLong)))
    }

    "n.render" >> {
      prop((n: ULong) => n.render == n.toBigInt.toString)
    }

    "n.toString = n.toBigInt.toString" >> {
      prop((n: ULong) => n.toString == n.toBigInt.toString)
    }

    "Order" >> {
      implicit val cogen: Cogen[ULong] = Cogen[BigInt].contramap[ULong](_.toBigInt)
      checkAll("ULong", OrderTests[ULong].order)
    }

  }

}
