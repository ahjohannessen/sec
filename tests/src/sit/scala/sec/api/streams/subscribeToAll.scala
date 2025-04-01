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
package api

import scala.concurrent.duration.*
import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import fs2.Stream
import sec.syntax.all.*

class SubscribeToAllSuite extends SnSuite:

  import StreamState.*

  val streamPrefix                       = s"streams_subscribe_to_all_${genIdentifier}_"
  val fromBeginning: Option[LogPosition] = Option.empty
  val fromEnd: Option[LogPosition]       = LogPosition.End.some

  group("streams do not exist prior to subscribing") {

    def mkId(suffix: String): StreamId =
      genStreamId(s"${streamPrefix}non_existing_stream_${suffix}_")

    def write(id: StreamId, data: Nel[EventData]) =
      Stream.eval(streams.appendToStream(id, NoStream, data))

    def subscribe(exclusiveFrom: Option[LogPosition], filter: StreamId => Boolean) = streams
      .subscribeToAll(exclusiveFrom)
      .filter(e => e.streamId.isNormal && filter(e.streamId))

    def run(exclusiveFrom: Option[LogPosition]) = {

      val s1       = mkId("s1")
      val s2       = mkId("s2")
      val s1Events = genEvents(10)
      val s2Events = genEvents(10)
      val count    = (s1Events.size + s2Events.size).toLong

      val writeBoth = (write(s1, s1Events) ++ write(s2, s2Events)).delayBy(1.second)
      val run       = subscribe(exclusiveFrom, Set(s1, s2).contains).concurrently(writeBoth)

      run.take(count).compile.toList.map { events =>
        assertEquals(events.size.toLong, count)
        assertEquals(events.filter(_.streamId == s1).map(_.eventData).toNel, Some(s1Events))
        assertEquals(events.filter(_.streamId == s2).map(_.eventData).toNel, Some(s2Events))
      }

    }

    test("from beginning")(run(fromBeginning))

    test("from log position") {
      val initId  = mkId("init")
      val prepare = write(initId, genEvents(10)).map(_.logPosition)

      prepare.compile.lastOrError >>= { pos => run(pos.some) }
    }

    test("from end")(run(fromEnd))

  }

  group("streams exist prior to subscribing") {

    def mkId(suffix: String): StreamId =
      genStreamId(s"${streamPrefix}existing_stream_${suffix}_")

    def write(id: StreamId, data: Nel[EventData], st: StreamState = NoStream) =
      Stream.eval(streams.appendToStream(id, st, data))

    def subscribe(exclusiveFrom: Option[LogPosition], filter: StreamId => Boolean) =
      streams.subscribeToAll(exclusiveFrom).filter(e => e.streamId.isNormal && filter(e.streamId))

    val s1Before = genEvents(12)
    val s2Before = genEvents(8)

    val s1After = genEvents(3)
    val s2After = genEvents(7)

    val before = s1Before.concatNel(s2Before)
    val after  = s1After.concatNel(s2After)
    val all    = before.concatNel(after)

    def writeBefore(s1: StreamId, s2: StreamId) =
      write(s1, s1Before) >>= { wa => write(s2, s2Before).map(wb => (wa, wb)) }

    def writeAfter(s1: StreamId, r1: StreamState, s2: StreamId, r2: StreamState) =
      (write(s1, s1After, r1) >> write(s2, s2After, r2)).delayBy(1.second)

    def run(exclusiveFrom: Option[LogPosition], s1: StreamId, s2: StreamId) =
      writeBefore(s1, s2) >>= { case (wa, wb) =>
        subscribe(exclusiveFrom, Set(s1, s2).contains)
          .concurrently(writeAfter(s1, wa.streamPosition, s2, wb.streamPosition))
          .map(_.eventData)
      }

    test("from beginning") {

      val s1    = mkId("s1_begin")
      val s2    = mkId("s2_begin")
      val count = all.size.toLong

      assertIO(run(fromBeginning, s1, s2).take(count).compile.toList.map(_.toNel), Some(all))
    }

    test("from log position") {

      val s1       = mkId("s1_pos")
      val s2       = mkId("s2_pos")
      val expected = s2Before.concatNel(after)
      val count    = expected.size.toLong

      val result = writeBefore(s1, s2) >>= { case (wrs1, wrs2) =>
        subscribe(wrs1.logPosition.some, Set(s1, s2).contains)
          .concurrently(writeAfter(s1, wrs1.streamPosition, s2, wrs2.streamPosition))
          .map(_.eventData)
          .take(count)
      }

      assertIO(result.compile.toList.map(_.toNel), Some(expected))
    }

    test("from end") {

      val s1    = mkId("s1_end")
      val s2    = mkId("s2_end")
      val count = after.size.toLong

      assertIO(run(fromEnd, s1, s2).take(count).compile.toList.map(_.toNel), Some(after))
    }

  }
