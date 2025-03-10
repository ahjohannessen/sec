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
package api

import java.util.UUID
import scala.concurrent.duration.*
import scodec.bits.ByteVector
import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import cats.effect.IO
import fs2.Stream
import sec.syntax.all.*
import sec.api.exceptions.*
import sec.helpers.implicits.*

class AppendToStreamSuite extends SnSuite:

  import StreamState.*
  import StreamPosition.*

  val streamPrefix = s"streams_append_to_stream_${genIdentifier}_"

  group("create stream on first write if does not exist") {

    val events = genEvents(1)

    def run(expectedState: StreamState) =

      val id = genStreamId(s"${streamPrefix}non_existing_${mkSnakeCase(expectedState.render)}_")

      streams.appendToStream(id, expectedState, events) >>= { wr =>
        streams.readStreamForwards(id, maxCount = 2).compile.toList.map { el =>
          assertEquals(el.map(_.eventData).toNel, events.some)
          assertEquals(wr.streamPosition, Start)
        }
      }

    test("works with any expected stream state")(run(Any))

    test("works with no stream expected stream state")(run(NoStream))

    test("raises with exact expected stream state") {
      run(Start).attempt.map {
        case Left(wes: WrongExpectedState) =>
          assertEquals(wes.expected, Start)
          assertEquals(wes.actual, NoStream)
        case other => fail(s"Expected Left with WrongExpectedState, got $other")
      }
    }

    test("raises with stream exists expected stream state") {
      run(StreamExists).attempt.map {
        case Left(wes: WrongExpectedState) =>
          assertEquals(wes.expected, StreamExists)
          assertEquals(wes.actual, NoStream)
        case other => fail(s"Expected Left with WrongExpectedState, got $other")
      }
    }

  }

  group("multiple idempotent writes") {

    test("with unique uuids") {

      val id     = genStreamId(s"${streamPrefix}multiple_idempotent_writes_")
      val events = genEvents(4)
      val write  = streams.appendToStream(id, Any, events)

      write >>= { first =>
        write.map { second =>
          assertEquals(first.streamPosition, second.streamPosition)
          assertEquals(first.streamPosition, StreamPosition(3L))
        }
      }

    }

    test("with same uuids (bug in ESDB)") {

      val id     = genStreamId(s"${streamPrefix}multiple_idempotent_writes_same_uuid_")
      val event  = genEvents(1).head
      val events = Nel.of(event, List.fill(5)(event)*)
      val write  = streams.appendToStream(id, Any, events)

      assertIO(write.map(_.streamPosition), StreamPosition(5))
    }

  }

  group("multiple writes of multiple events with same uuids using expected stream state") {

    def run(expectedState: StreamState, secondExpectedState: StreamState) =

      val st     = mkSnakeCase(expectedState.render)
      val id     = genStreamId(s"${streamPrefix}multiple_writes_multiple_events_same_uuid_${st}_")
      val event  = genEvents(1).head
      val events = Nel.of(event, List.fill(5)(event)*)
      val write  = streams.appendToStream(id, expectedState, events)

      write >>= { first =>
        write.map { second =>
          assertEquals(first.streamPosition, StreamPosition(5))
          assertEquals((second.streamPosition: StreamState), secondExpectedState)
        }
      }

    test("any then next expected stream state is unreliable")(run(Any, Start))

    test("no stream then next expected stream state is correct")(run(NoStream, StreamPosition(5)))

  }

  group("append to tombstoned stream raises") {

    def run(expectedState: StreamState) =
      val st     = mkSnakeCase(expectedState.render)
      val id     = genStreamId(s"${streamPrefix}tombstoned_stream_${st}_")
      val events = genEvents(1)
      val delete = streams.tombstone(id, NoStream)
      val write  = streams.appendToStream(id, expectedState, events)

      assertIO(delete >> write.attempt, StreamDeleted(id.stringValue).asLeft)

    test("with correct expected stream state")(run(NoStream))

    test("with any expected stream state")(run(Any))

    test("with stream exists expected stream state")(run(StreamExists))

    test("with incorrect expected stream state")(run(StreamPosition(5)))

  }

  group("append to existing stream") {

    def run(sndExpectedState: StreamState): IO[StreamPosition] =

      val st                      = mkSnakeCase(sndExpectedState.render)
      val id                      = genStreamId(s"${streamPrefix}existing_stream_with_${st}_")
      def write(esr: StreamState) = streams.appendToStream(id, esr, genEvents(1))

      assertIO(write(NoStream).map(_.streamPosition), Start) *>
        write(sndExpectedState).map(_.streamPosition)

    test("works with correct expected stream state") {
      assertIO(run(Start), StreamPosition(1))
    }

    test("works with any expected stream state") {
      assertIO(run(Any), StreamPosition(1))
    }

    test("works with stream exists expected stream state") {
      assertIO(run(StreamExists), StreamPosition(1))
    }

    test("raises with incorrect expected stream state") {
      run(StreamPosition(1L)).attempt.map {
        case Left(wes: WrongExpectedState) =>
          assertEquals(wes.expected, StreamPosition(1L))
          assertEquals(wes.actual, Start)
        case other => fail(s"Expected Left with WrongExpectedState, got $other")
      }
    }

  }

  test("append to stream with multiple events and stream exists expected stream state ") {

    val id      = genStreamId(s"${streamPrefix}multiple_events_and_stream_exists_")
    val events  = genEvents(5)
    val writes  = events.toList.map(e => streams.appendToStream(id, Any, Nel.one(e)))
    val prepare = Stream.eval(writes.sequence).compile.drain
    val action  = prepare >> streams.appendToStream(id, StreamExists, genEvents(1)).map(_.streamPosition)

    assertIO(action, StreamPosition(5))
  }

  test("append to stream with stream exists expected version works if metadata stream exists") {

    val id     = genStreamId(s"${streamPrefix}stream_exists_and_metadata_stream_exists_")
    val meta   = metaStreams.setMaxAge(id, NoStream, 10.seconds)
    val write  = streams.appendToStream(id, StreamExists, genEvents(1))
    val action = meta >> write.map(_.streamPosition)

    assertIO(action, Start)

  }

  test("append to deleted stream with stream exists expected version raises") {

    val st = mkSnakeCase(StreamExists.render)
    val id = genStreamId(s"${streamPrefix}stream_exists_and_deleted_${st}_")

    val run = streams.appendToStream(id, NoStream, genEvents(1)) >>
      streams.delete(id, StreamExists) >>
      streams.appendToStream(id, StreamExists, genEvents(1))

    interceptMessageIO[StreamDeleted](StreamDeleted(id.stringValue).getMessage)(run)
  }

  test("append multiple events at once") {

    val id       = genStreamId(s"${streamPrefix}multiple_events_at_once_")
    val events   = genEvents(100)
    val expected = StreamPosition(99)

    assertIO(streams.appendToStream(id, NoStream, events).map(_.streamPosition), expected)
  }

  group("append events with size") {

    val max = 1024 * 1024 // Default ESDB setting

    // ((data length) + (metadata length) + (eventType length * 2);)

    def mkEvent(sizeBytes: Int): IO[EventData] = IO(UUID.randomUUID()).map { uuid =>

      val etName   = "et"
      val et       = EventType(etName).unsafeGet
      val etSize   = etName.length * 2
      val data     = ByteVector.fill((sizeBytes - etSize).toLong)(0)
      val metadata = ByteVector.empty
      val ct       = ContentType.Binary

      EventData(et, uuid, data, metadata, ct)
    }

    test("less than or equal max append size works") {

      val id = genStreamId(s"${streamPrefix}append_size_less_or_equal_bytes_")

      val equal  = List(mkEvent(max / 2), mkEvent(max / 2)).sequence.map(Nel.fromListUnsafe)
      val equalA = equal >>= (streams.appendToStream(id, NoStream, _))

      val lessThan  = List(mkEvent(max / 4), mkEvent(max / 2)).sequence.map(Nel.fromListUnsafe)
      val lessThanA = lessThan >>= (streams.appendToStream(id, StreamPosition(1L), _))

      val t1 = assertIOBoolean(equalA.attempt.map(_.isRight))
      val t2 = assertIOBoolean(lessThanA.attempt.map(_.isRight))

      t1 *> t2
    }

    test("greater than max append size raises") {

      val id          = genStreamId(s"${streamPrefix}append_size_exceeds_bytes_")
      val greaterThan = List(mkEvent(max / 2), mkEvent(max / 2), mkEvent(max + 1)).sequence.map(Nel.fromListUnsafe)

      greaterThan >>= { events =>
        streams.appendToStream(id, NoStream, events).attempt.map {
          assertEquals(_, MaximumAppendSizeExceeded(max.some).asLeft)
        }
      }

    }

  }

  group("append to implicitly created streams") {

    /*
     * sequence - events written to stream
     * 0em1 - event number 0 written with expected state -1 (minus 1)
     * 1any - event number 1 written with expected state any
     * S_0em1_1em1_E - START bucket, two events in bucket, END bucket
     *
     *   See: https://github.com/EventStore/EventStore/blob/master/src/EventStore.Core.Tests/ClientAPI/appending_to_implicitly_created_stream.cs
     */

    def mkId = genStreamId(s"${streamPrefix}implicitly_created_stream_")

    test("sequence 0em1 1e0 2e1 3e2 4e3 5e4") {

      def run(nextExpected: StreamState) =

        val id     = mkId
        val events = genEvents(6)

        for
          _ <- streams.appendToStream(id, NoStream, events)
          _ <- streams.appendToStream(id, nextExpected, Nel.one(events.head))
          e <- streams.readStreamForwards(id, maxCount = events.size + 1L).compile.toList
        yield assertEquals(e.size, events.size)

      // 0em1 is idempotent
      val t1 = run(NoStream)

      // 0any is idempotent
      val t2 = run(Any)

      t1 *> t2

    }

    test("sequence 0em1 1e0 2e1 3e2 4e3 5e4 0e5 is non idempotent") {

      val id     = mkId
      val events = genEvents(6)

      for
        _ <- streams.appendToStream(id, NoStream, events)
        _ <- streams.appendToStream(id, StreamPosition(5), Nel.one(events.head))
        e <- streams.readStreamForwards(id, maxCount = events.size + 2L).compile.toList
      yield assertEquals(e.size, events.size + 1)

    }

    test("sequence 0em1 1e0 2e1 3e2 4e3 5e4 0e6 raises") {

      val id     = mkId
      val events = genEvents(6)

      val result = for
        _ <- streams.appendToStream(id, NoStream, events)
        _ <- streams.appendToStream(id, StreamPosition(6), Nel.one(events.head))
      yield ()

      assertIO(result.attempt, Left(WrongExpectedState(id, StreamPosition(6), StreamPosition(5))))
    }

    test("sequence 0em1 1e0 2e1 3e2 4e3 5e4 0e4 raises") {

      val id     = mkId
      val events = genEvents(6)

      val result = for
        _ <- streams.appendToStream(id, NoStream, events)
        _ <- streams.appendToStream(id, StreamPosition(4), Nel.one(events.head))
      yield ()

      assertIO(result.attempt, Left(WrongExpectedState(id, StreamPosition(4), StreamPosition(5))))
    }

    test("sequence 0em1 0e0 is non idempotent") {

      val id     = mkId
      val events = genEvents(1)

      for
        _ <- streams.appendToStream(id, NoStream, events)
        _ <- streams.appendToStream(id, Start, Nel.one(events.head))
        e <- streams.readStreamForwards(id, maxCount = events.size + 2L).compile.toList
      yield assertEquals(e.size, events.size + 1)

    }

    test("sequence 0em1") {

      def run(nextExpected: StreamState) =

        val id     = mkId
        val events = genEvents(1)

        for
          _ <- streams.appendToStream(id, NoStream, events)
          _ <- streams.appendToStream(id, nextExpected, Nel.one(events.head))
          e <- streams.readStreamForwards(id, maxCount = events.size + 1L).compile.toList
        yield assertEquals(e.size, events.size)

      // 0em1 is idempotent
      val t1 = run(NoStream)

      // 0any is idempotent
      val t2 = run(Any)

      t1 *> t2

    }

    test("sequence 0em1 1e0 2e1 1any 1any is idempotent") {

      val id = mkId
      val e1 = genEvent
      val e2 = genEvent
      val e3 = genEvent

      for
        _ <- streams.appendToStream(id, NoStream, Nel.of(e1, e2, e3))
        _ <- streams.appendToStream(id, Any, Nel.of(e2))
        _ <- streams.appendToStream(id, Any, Nel.of(e2))
        e <- streams.readStreamForwards(id, maxCount = 4).compile.toList
      yield assertEquals(e.size, 3)

    }

    test("sequence S 0em1 1em1 E") {

      def run(nextState: StreamState, onlyLast: Boolean) =

        val id         = mkId
        val e1         = genEvent
        val e2         = genEvent
        val events     = Nel.of(e1, e2)
        val nextEvents = if (onlyLast) Nel.one(e2) else events

        for
          _ <- streams.appendToStream(id, NoStream, events)
          _ <- streams.appendToStream(id, nextState, nextEvents)
          e <- streams.readStreamForwards(id, maxCount = events.size + 1L).compile.toList
        yield assertEquals(e.size, events.size)

      // S 0em1 E is idempotent
      val t1 = run(NoStream, onlyLast = false)

      // S 0any E is idempotent
      val t2 = run(Any, onlyLast = false)

      // S 1e0  E is idempotent
      val t3 = run(Start, onlyLast = true)

      // S 1any E is idempotent
      val t4 = run(Any, onlyLast = true)

      t1 *> t2 *> t3 *> t4

    }

    test("sequence S 0em1 1em1 E S 0em1 1em1 2em1 E raises") {

      val id     = mkId
      val e1     = genEvent
      val e2     = genEvent
      val e3     = genEvent
      val first  = Nel.of(e1, e2)
      val second = Nel.of(e1, e2, e3)

      streams.appendToStream(id, NoStream, first) *> assertIO(
        streams.appendToStream(id, NoStream, second).attempt,
        Left(WrongExpectedState(id, NoStream, StreamPosition(1L)))
      )

    }

  }
