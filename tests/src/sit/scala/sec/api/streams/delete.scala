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
import cats.syntax.all.*
import cats.effect.IO
import io.circe.Json
import sec.syntax.all.*
import sec.api.exceptions.*
import helpers.implicits.*

class DeleteSuite extends SnSuite:

  import StreamState.*
  import StreamPosition.*

  val streamPrefix = s"streams_delete_${genIdentifier}_"

  group("delete a stream that does not exist") {

    test("raises with wrong expected stream state") {

      val es: StreamState = Start
      val st              = mkSnakeCase(es.render)
      val id              = genStreamId(s"${streamPrefix}non_existing_stream_with_expected_state${st}_")
      val expectedMsg     = WrongExpectedState.msg(id, es, NoStream)

      interceptMessageIO[WrongExpectedState](expectedMsg)(streams.delete(id, es))
    }

  }

  test("delete a stream should return log position") {

    val id     = genStreamId(s"${streamPrefix}return_log_position_")
    val events = genEvents(1)

    for
      wr  <- streams.appendToStream(id, NoStream, events)
      pos <- streams.readAllBackwards(maxCount = 1).compile.lastOrError.map(_.logPosition)
      dr  <- streams.delete(id, wr.streamPosition)
    yield assert(dr.logPosition > pos)

  }

  test("delete a stream and reading raises") {

    val id     = genStreamId(s"${streamPrefix}reading_raises_")
    val events = genEvents(1)

    val run = for
      wr <- streams.appendToStream(id, NoStream, events)
      _  <- streams.delete(id, wr.streamPosition)
      _  <- streams.readStreamForwards(id, maxCount = 1).compile.drain
    yield ()

    interceptIO[StreamNotFound](run)

  }

  test("delete a stream and tombstone works as expected") {

    val id     = genStreamId(s"${streamPrefix}and_tombstone_it_")
    val events = genEvents(2)

    for
      wr  <- streams.appendToStream(id, NoStream, events)
      _   <- streams.delete(id, wr.streamPosition)
      _   <- streams.tombstone(id, Any)
      rat <- streams.readStreamForwards(id, maxCount = 2).compile.drain.attempt
      mat <- metaStreams.getMetadata(id).attempt
      aat <- streams.appendToStream(id, Any, genEvents(1)).attempt
    yield
      (rat, mat, aat) match
        case (Left(r: StreamDeleted), Left(m: StreamDeleted), Left(a: StreamDeleted)) =>
          assertEquals(r.streamId, id.stringValue)
          assertEquals(m.streamId, id.metaId.stringValue)
          assertEquals(a.streamId, id.stringValue)
        case other => fail(s"Did not expect $other")

  }

  group("delete a stream and recreate with") {

    def run(expectedState: StreamState) =

      val st           = mkSnakeCase(expectedState.render)
      val id           = genStreamId(s"${streamPrefix}and_recreate_with_expected_state_${st}_")
      val beforeEvents = genEvents(1)
      val afterEvents  = genEvents(3)

      for
        wr1 <- streams.appendToStream(id, NoStream, beforeEvents)
        _   <- streams.delete(id, wr1.streamPosition)
        wr2 <- streams.appendToStream(id, expectedState, afterEvents)
        _   <- IO.sleep(100.millis) // Workaround for ES github issue #1744
        evt <- streams.readStreamForwards(id, maxCount = 3).compile.toList
        tbm <- metaStreams.getTruncateBefore(id)
      yield
        assertEquals(wr1.streamPosition, Start)
        assertEquals(wr2.streamPosition, StreamPosition(3))

        assertEquals(evt.size, 3)
        assertEquals(evt.map(_.eventData).toNel, afterEvents.some)
        assertEquals(evt.map(_.streamPosition), List(StreamPosition(1), StreamPosition(2), StreamPosition(3)))

        assertEquals(tbm, Some(MetaStreams.Result(StreamPosition(1), StreamPosition(1).some)))

    test("no stream expected stream state") {
      run(StreamState.NoStream)
    }

    test("any expected stream state") {
      run(StreamState.Any)
    }

    test("exact expected stream state") {
      run(StreamPosition.Start)
    }

  }

  test("delete a stream and recreate preserves metadata except truncate before") {

    val id           = genStreamId(s"${streamPrefix}and_recreate_preserves_metadata_")
    val beforeEvents = genEvents(2)
    val afterEvents  = genEvents(3)

    val metadata = StreamMetadata.empty
      .withAcl(StreamAcl.empty.withDeleteRoles(Set("some-role")))
      .withMaxCount(MaxCount(100).unsafeGet)
      .withTruncateBefore(StreamPosition(Long.MaxValue))
      .withCustom("k1" -> Json.True, "k2" -> Json.fromInt(17), "k3" -> Json.fromString("some value"))

    for
      swr    <- streams.appendToStream(id, NoStream, beforeEvents)
      mwr    <- metaStreams.setMetadata(id, NoStream, metadata)
      _      <- streams.appendToStream(id, StreamPosition(1), afterEvents)
      _      <- IO.sleep(1.second) // Workaround for ES github issue #1744
      events <- streams.readStreamForwards(id, maxCount = 3).compile.toList
      meta   <- metaStreams.getMetadata(id)
    yield

      assertEquals(swr.streamPosition, StreamPosition(1))
      assertEquals(mwr.streamPosition, Start)

      assertEquals(events.size, afterEvents.size)
      assertEquals(events.map(_.streamPosition), List(StreamPosition(2), StreamPosition(3), StreamPosition(4)))

      meta.fold(fail("Expected some metadata")) { m =>
        assertEquals(m.streamPosition, StreamPosition(1))
        assertEquals(m.data, metadata.withTruncateBefore(StreamPosition(2)))
      }

  }

  test("delete a stream and recreate raises if not first write") {

    val id = genStreamId(s"${streamPrefix}and_recreate_only_first_write_")

    for
      wr1 <- streams.appendToStream(id, NoStream, genEvents(2))
      _   <- streams.delete(id, wr1.streamPosition)
      wr2 <- streams.appendToStream(id, NoStream, genEvents(3))
      wr3 <- streams.appendToStream(id, StreamState.NoStream, genEvents(1)).attempt
    yield
      assertEquals(wr1.streamPosition, StreamPosition(1))
      assertEquals(wr2.streamPosition, StreamPosition(4))
      assertEquals(wr3, Left(WrongExpectedState(id, NoStream, StreamPosition(4L))))

  }

  test("delete a stream and recreate with multiple appends and expected stream state any") {

    val id      = genStreamId(s"${streamPrefix}and_recreate_multiple_writes_with_any_expected_state_")
    val events0 = genEvents(2)
    val events1 = genEvents(3)
    val events2 = genEvents(2)

    for
      wr0    <- streams.appendToStream(id, NoStream, events0)
      _      <- streams.delete(id, wr0.streamPosition)
      wr1    <- streams.appendToStream(id, Any, events1)
      wr2    <- streams.appendToStream(id, Any, events2)
      events <- streams.readStreamForwards(id, maxCount = 5).compile.toList
      tbr    <- metaStreams.getTruncateBefore(id)
    yield
      assertEquals(wr0.streamPosition, StreamPosition(1))
      assertEquals(wr1.streamPosition, StreamPosition(4))
      assertEquals(wr2.streamPosition, StreamPosition(6))

      assertEquals(events.size, 5)
      assertEquals(events.map(_.eventData), events1.concatNel(events2).toList)
      assertEquals(events.map(_.streamPosition), (2L to 6L).map(StreamPosition(_)).toList)

      tbr.fold(fail("Expected some truncated before")) { result =>
        assertEquals(result.data, Some(StreamPosition(2)))
        assertEquals(result.streamPosition, StreamPosition(1))
      }

  }

  test("delete a stream and recreate on empty when metadata set") {

    val id = genStreamId(s"${streamPrefix}and_recreate_on_empty_when_metadata_set_")

    val metadata = StreamMetadata.empty
      .withMaxCount(MaxCount(100).unsafeGet)
      .withAcl(StreamAcl.empty.withDeleteRoles(Set("some-role")))
      .withTruncateBefore(StreamPosition(Long.MaxValue))
      .withCustom("k1" -> Json.True, "k2" -> Json.fromInt(17), "k3" -> Json.fromString("some value"))

    for
      _    <- streams.appendToStream(id, NoStream, genEvents(1))
      _    <- streams.delete(id, StreamExists)
      mw   <- metaStreams.setMetadata(id, Start, metadata)
      read <- streams.readStreamForwards(id, maxCount = 1).compile.toList.attempt
      meta <- metaStreams.getMetadata(id)
    yield
      assertEquals(mw.streamPosition, StreamPosition(1))
      assertEquals(read, Right(Nil))

      meta.fold(fail("Expected some metadata")) { m =>
        assertEquals(m.streamPosition, StreamPosition(2))
        assertEquals(m.data, metadata.withTruncateBefore(StreamPosition(1)))
      }

  }

  test("delete a stream and recreate on non-empty when metadata set") {

    val id     = genStreamId(s"${streamPrefix}and_recreate_on_non_empty_when_metadata_set_")
    val events = genEvents(2)

    val metadata = StreamMetadata.empty
      .withMaxCount(MaxCount(100).unsafeGet)
      .withAcl(StreamAcl.empty.withDeleteRoles(Set("some-role")))
      .withCustom("k1" -> Json.True, "k2" -> Json.fromInt(17), "k3" -> Json.fromString("some value"))

    for
      sw1  <- streams.appendToStream(id, NoStream, events)
      _    <- streams.delete(id, sw1.streamPosition)
      mw1  <- metaStreams.setMetadata(id, Start, metadata)
      read <- streams.readStreamForwards(id, maxCount = 2).compile.toList
      meta <- metaStreams.getMetadata(id)
    yield
      assertEquals(sw1.streamPosition, StreamPosition(1))
      assertEquals(mw1.streamPosition, StreamPosition(1))
      assertEquals(read.size, 0)

      meta.fold(fail("Expected some metadata")) { m =>
        assertEquals(m.data, metadata.withTruncateBefore(StreamPosition(events.size.toLong)))
      }

  }
