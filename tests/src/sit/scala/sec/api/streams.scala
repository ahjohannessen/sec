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
import java.{util => ju}
import scala.concurrent.duration._
import cats._
import cats.data.{NonEmptyList => Nel}
import cats.effect.{IO, Ref}
import cats.syntax.all._
import fs2._
import io.circe.Json
import scodec.bits.ByteVector
import sec.api.Direction._
import sec.api.exceptions._
import sec.syntax.all._
import helpers.text.mkSnakeCase
import helpers.implicits._
import helpers.implicits.munit._

class ReadAllSuite extends SnSpec {

  import LogPosition._

  private val streamPrefix    = s"streams_read_all_${genIdentifier}_"
  private val eventTypePrefix = s"sec.$genIdentifier.Event"

  private val id1     = genStreamId(streamPrefix)
  private val id2     = genStreamId(streamPrefix)
  private val events1 = genEvents(250, eventTypePrefix)
  private val events2 = genEvents(250, eventTypePrefix)
  private val written = events1 ::: events2

  def read(from: LogPosition, direction: Direction, maxCount: Long = 1): IO[List[AllEvent]] =
    streams.readAll(from, direction, maxCount, resolveLinkTos = false).compile.toList

  def testForwards(name: String)(body: => Any): Unit =
    test(s"forwards / $name")(body)

  def testBackwards(name: String)(body: => Any): Unit =
    test(s"backwards / $name")(body)

  ///

  test("init") {
    streams.appendToStream(id1, StreamState.NoStream, events1) *>
      streams.appendToStream(id2, StreamState.NoStream, events2)
  }

  ///

  testForwards("reading from start yields events") {
    read(Start, Forwards, written.size.toLong).map(_.size shouldEqual written.size)
  }

  testForwards("reading from end yields no events") {
    read(End, Forwards, 10).map(_.isEmpty shouldEqual true)
  }

  testForwards("events are in same order as written") {
    read(Start, Forwards, Int.MaxValue - 1)
      .map(_.collect { case e if e.streamId == id1 => e.eventData })
      .map(_ shouldEqual events1.toList)
  }

  testForwards("max count <= 0 yields no events") {
    read(Start, Forwards, 0).map(_.isEmpty shouldEqual true)
  }

  testForwards("max count is respected") {
    streams
      .readAll(Start, Forwards, events1.length / 2L, resolveLinkTos = false)
      .take(events1.length.toLong)
      .compile
      .toList
      .map(_.size shouldEqual events1.length / 2)
  }

  def deleted(normalDelete: Boolean): IO[Unit] = {

    val suffix = if (normalDelete) "" else "tombstoned"
    val id     = genStreamId(s"streams_read_all_deleted_${suffix}_")
    val events = genEvents(10)
    val write  = streams.appendToStream(id, StreamState.NoStream, events)

    def delete(er: StreamPosition.Exact) =
      if (normalDelete) streams.delete(id, er) else streams.tombstone(id, er)

    val decodeJson: EventData => IO[StreamMetadata] =
      _.data.decodeUtf8.liftTo[IO] >>= {
        io.circe.parser.decode[StreamMetadata](_).liftTo[IO]
      }

    val verifyDeleted =
      streams.readStreamForwards(id, maxCount = 1).compile.drain.recoverWith {
        case e: StreamNotFound if normalDelete && e.streamId.eqv(id.stringValue) => IO.unit
        case e: StreamDeleted if !normalDelete && e.streamId.eqv(id.stringValue) => IO.unit
      }

    val read = streams
      .readAllForwards()
      .filter(e => e.streamId.eqv(id) || e.streamId.eqv(id.metaId))
      .compile
      .toList

    val setup = write >>= { wr => delete(wr.streamPosition).void >> verifyDeleted >> read }

    def verify(es: List[AllEvent]) =
      es.lastOption.toRight(new RuntimeException("expected metadata")).liftTo[IO] >>= { ts =>
        if (normalDelete) {
          es.dropRight(1).map(_.eventData) shouldEqual events.toList
          ts.streamId shouldEqual id.metaId
          ts.eventData.eventType shouldEqual EventType.StreamMetadata
          decodeJson(ts.record.eventData).map(_.state.truncateBefore.map(_.value) shouldEqual Some(Long.MaxValue))
        } else {
          es.dropRight(1).map(_.eventData) shouldEqual events.toList
          ts.streamId shouldEqual id
          ts.eventData.eventType shouldEqual EventType.StreamDeleted
          IO.unit
        }
      }

    setup >>= verify

  }

  testForwards("deleted stream")(deleted(normalDelete = true))
  testForwards("tombstoned stream")(deleted(normalDelete = false))

  testForwards("max count deleted events are not resolved") {

    val deletedId               = genStreamId("streams_read_all_linkto_deleted_")
    val linkId                  = genStreamId("streams_read_all_linkto_link_")
    def encode(content: String) = ByteVector.encodeUtf8(content).unsafe

    def linkData(number: Long) =
      Nel.one(
        EventData(
          EventType.LinkTo,
          ju.UUID.randomUUID(),
          encode(s"$number@${deletedId.stringValue}"),
          ByteVector.empty,
          ContentType.Binary
        ))

    def append(id: StreamId, data: Nel[EventData]) =
      streams.appendToStream(id, StreamState.Any, data)

    def readLink(resolve: Boolean) =
      streams.readStreamForwards(linkId, maxCount = 3, resolveLinkTos = resolve).map(_.eventData).compile.toList

    val e1 = genEvents(1)
    val e2 = genEvents(1)
    val e3 = genEvents(1)

    val l1 = linkData(0)
    val l2 = linkData(1)
    val l3 = linkData(2)

    append(deletedId, e1) >>
      metaStreams.setMaxCount(deletedId, StreamState.NoStream, 2) >>
      append(deletedId, e2) >>
      append(deletedId, e3) >>
      append(linkId, l1) >>
      append(linkId, l2) >>
      append(linkId, l3) >>
      readLink(resolve = true).map(_ shouldEqual (e2 ::: e3).toList) >>
      readLink(resolve = false).map(_ shouldEqual (l1 ::: l2 ::: l3).toList)
  }

  ///

  testBackwards("reading from start yields no events") {
    read(Start, Backwards).map(_.isEmpty shouldEqual true)
  }

  testBackwards("reading from end yields events") {
    read(End, Backwards).map(_.nonEmpty shouldEqual true)
  }

  testBackwards("events are in reverse order as written") {
    read(End, Backwards, Int.MaxValue - 1)
      .map(_.collect { case e if e.streamId == id1 => e.eventData })
      .map(_.reverse shouldEqual events1.toList)
  }

  testBackwards("max count <= 0 yields no events") {
    read(End, Backwards, 0).map(_.isEmpty shouldEqual true)
  }

  testBackwards("max count is respected") {
    streams
      .readAll(End, Backwards, events1.length / 2L, resolveLinkTos = false)
      .take(events1.length.toLong)
      .compile
      .toList
      .map(_.size shouldEqual events1.length / 2)
  }

}

class ReadStreamSuite extends SnSpec {

  import StreamPosition._

  private val streamPrefix = s"streams_read_stream_${genIdentifier}_"
  private val id           = genStreamId(streamPrefix)
  private val events       = genEvents(25)

  def read(id: StreamId, from: StreamPosition, direction: Direction, count: Long = 50): IO[List[StreamEvent]] =
    streams.readStream(id, from, direction, count, resolveLinkTos = false).compile.toList

  def readData(id: StreamId, from: StreamPosition, direction: Direction, count: Long = 50): IO[List[EventData]] =
    read(id, from, direction, count).map(_.map(_.eventData))

  def testForwards(name: String)(body: => Any): Unit =
    test(s"forwards / $name")(body)

  def testBackwards(name: String)(body: => Any): Unit =
    test(s"backwards / $name")(body)

  ///

  test("init")(streams.appendToStream(id, StreamState.NoStream, events))

  ///

  testForwards("reading from start yields events") {
    read(id, Start, Forwards).map(_.size shouldEqual events.size)
  }

  testForwards("reading from start with count <= 0 yields no events") {
    read(id, Start, Forwards, -1L).map(_.isEmpty shouldEqual true)
    read(id, Start, Forwards, 0).map(_.isEmpty shouldEqual true)
  }

  testForwards("reading from end yields no events") {
    read(id, End, Forwards).map(_.isEmpty shouldEqual true)
  }

  testForwards("events are in same order as written") {
    readData(id, Start, Forwards).map(_ shouldEqual events.toList)
  }

  testForwards("reading non-existing stream raises stream not found") {

    val id = genStreamId(s"${streamPrefix}non_existing_forwards_")
    val ex = StreamNotFound(id.stringValue)

    read(id, Start, Forwards, 1).attempt.map(_ shouldEqual Left(ex))
  }

  testForwards("reading deleted stream raises stream not found") {

    val id = genStreamId(s"${streamPrefix}stream_deleted_forwards_")
    val ex = StreamNotFound(id.stringValue)

    streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
      streams.delete(id, StreamState.Any) *>
      read(id, Start, Forwards, 5).attempt.map(_ shouldEqual Left(ex))
  }

  testForwards("reading tombstoned stream raises stream deleted") {

    val id = genStreamId(s"${streamPrefix}stream_tombstoned_forwards_")
    val ex = StreamDeleted(id.stringValue)

    streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
      streams.tombstone(id, StreamState.Any) *>
      read(id, Start, Forwards, 5).attempt.map(_ shouldEqual Left(ex))
  }

  testForwards("reading single event from arbitrary stream position") {
    readData(id, exact(7), Forwards, 1).map(_.lastOption shouldEqual events.get(7))
  }

  testForwards("reading from arbitrary stream position") {
    readData(id, exact(3L), Forwards, 2).map(_ shouldEqual events.toList.slice(3, 5))
  }

  testForwards("max count is respected") {
    streams
      .readStream(id, Start, Forwards, events.length / 2L, resolveLinkTos = false)
      .take(events.length.toLong)
      .compile
      .toList
      .map(_.size shouldEqual events.length / 2)
  }

  ///

  testBackwards("reading one from start yields first event") {
    readData(id, Start, Backwards, 1).map { e =>
      e.lastOption shouldEqual Some(events.head)
      e.size shouldEqual 1
    }
  }

  testBackwards("reading one from end yields last event") {
    readData(id, End, Backwards, 1).map { es =>
      es.headOption shouldEqual Some(events.last)
      es.size shouldEqual 1
    }
  }

  testBackwards("reading from end with max count <= 0 yields no events") {
    read(id, End, Backwards, -1).map(_.isEmpty shouldEqual true)
    read(id, End, Backwards, 0).map(_.isEmpty shouldEqual true)
  }

  testBackwards("events are in reverse order as written") {
    readData(id, End, Backwards).map(_.reverse shouldEqual events.toList)
  }

  testBackwards("reading non-existing stream raises stream not found") {
    val id = genStreamId(s"${streamPrefix}non_existing_backwards_")
    val ex = StreamNotFound(id.stringValue)
    read(id, End, Backwards, 1).attempt.map(_ shouldEqual Left(ex))
  }

  testBackwards("reading deleted stream raises stream not found") {

    val id = genStreamId(s"${streamPrefix}stream_deleted_backwards_")
    val ex = StreamNotFound(id.stringValue)

    streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
      streams.delete(id, StreamState.Any) *>
      read(id, End, Backwards, 5).attempt.map(_ shouldEqual Left(ex))
  }

  testBackwards("reading tombstoned stream raises stream deleted") {

    val id = genStreamId(s"${streamPrefix}stream_tombstoned_backwards_")
    val ex = StreamDeleted(id.stringValue)

    streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
      streams.tombstone(id, StreamState.Any) *>
      read(id, End, Backwards, 5).attempt.map(_ shouldEqual Left(ex))
  }

  testBackwards("reading single event from arbitrary stream position") {
    readData(id, StreamPosition.exact(20), Backwards, 1).map(_.lastOption shouldEqual events.get(20))
  }

  testBackwards("reading from arbitrary stream position") {
    readData(id, StreamPosition.exact(3L), Backwards, 2).map(_ shouldEqual events.toList.slice(2, 4).reverse)
  }

  testBackwards("max count is respected") {
    streams
      .readStream(id, End, Backwards, events.length / 2L, resolveLinkTos = false)
      .take(events.length.toLong)
      .compile
      .toList
      .map(_.size shouldEqual events.length / 2)
  }

}

class AppendSuite extends SnSpec {

  val streamPrefix = s"streams_append_to_stream_${genIdentifier}_"

  test("create stream on first write if does not exist") {

    val events = genEvents(1)

    def test(expectedState: StreamState) = {

      val id = genStreamId(s"${streamPrefix}non_existing_${mkSnakeCase(expectedState.render)}_")

      streams.appendToStream(id, expectedState, events) >>= { wr =>
        streams.readStreamForwards(id, maxCount = 2).compile.toList.map { el =>
          el.map(_.eventData).toNel shouldEqual events.some
          wr.streamPosition shouldEqual StreamPosition.Start
        }
      }
    }

    // works with any expected stream state
    val t1 = test(StreamState.Any)

    // works with no stream expected stream state
    val t2 = test(StreamState.NoStream)

    // raises with exact expected stream state
    val t3 = test(StreamPosition.Start).attempt.map {
      case Left(WrongExpectedState(_, StreamPosition.Start, StreamState.NoStream)) => ()
      case r                                                                       => fail(s"dit not expect $r")
    }

    // raises with stream exists expected stream state
    val t4 = test(StreamState.StreamExists).attempt.map {
      case Left(WrongExpectedState(_, StreamState.StreamExists, StreamState.NoStream)) => ()
      case r                                                                           => fail(s"dit not expect $r")

    }

    t1 >> t2 >> t3 >> t4

  }

  test("multiple idempotent writes with unique uuids") {

    val id     = genStreamId(s"${streamPrefix}multiple_idempotent_writes_")
    val events = genEvents(4)
    val write  = streams.appendToStream(id, StreamState.Any, events)

    write >>= { first =>
      write.map { second =>
        first.streamPosition shouldEqual second.streamPosition
        first.streamPosition shouldEqual StreamPosition.exact(3L)
      }
    }

  }

  test("multiple idempotent writes with same uuids (bug in ESDB)") {

    val id     = genStreamId(s"${streamPrefix}multiple_idempotent_writes_same_uuid_")
    val event  = genEvents(1).head
    val events = Nel.of(event, List.fill(5)(event): _*)
    val write  = streams.appendToStream(id, StreamState.Any, events)

    write.map(_.streamPosition shouldEqual StreamPosition.exact(5))
  }

  test("multiple writes of multiple events with same uuids using expected stream state") {

    def test(expectedState: StreamState, secondExpectedState: StreamState) = {

      val st     = mkSnakeCase(expectedState.render)
      val id     = genStreamId(s"${streamPrefix}multiple_writes_multiple_events_same_uuid_${st}_")
      val event  = genEvents(1).head
      val events = Nel.of(event, List.fill(5)(event): _*)
      val write  = streams.appendToStream(id, expectedState, events)

      write >>= { first =>
        write.map { second =>
          first.streamPosition shouldEqual StreamPosition.exact(5)
          assert(Eq.eqv(second.streamPosition, secondExpectedState))
        }
      }

    }

    // any then next expected stream state is unreliable
    val t1 = test(StreamState.Any, StreamPosition.Start)

    // no stream then next expected stream state is correct
    val t2 = test(StreamState.NoStream, StreamPosition.exact(5))

    t1 >> t2
  }

  test("append to tombstoned stream raises") {

    def test(expectedState: StreamState) = {
      val st     = mkSnakeCase(expectedState.render)
      val id     = genStreamId(s"${streamPrefix}tombstoned_stream_${st}_")
      val events = genEvents(1)
      val delete = streams.tombstone(id, StreamState.NoStream)
      val write  = streams.appendToStream(id, expectedState, events)

      delete >> write.attempt.map(_ shouldEqual StreamDeleted(id.stringValue).asLeft)
    }

    // with correct expected stream state
    val t1 = test(StreamState.NoStream)

    // with any expected stream state
    val t2 = test(StreamState.Any)

    // with stream exists expected stream state
    val t3 = test(StreamState.StreamExists)

    // with incorrect expected stream state
    val t4 = test(StreamPosition.exact(5))

    t1 >> t2 >> t3 >> t4
  }

  test("append to existing stream") {

    def run(sndExpectedState: StreamState) = {

      val st                      = mkSnakeCase(sndExpectedState.render)
      val id                      = genStreamId(s"${streamPrefix}existing_stream_with_${st}_")
      def write(esr: StreamState) = streams.appendToStream(id, esr, genEvents(1))

      write(StreamState.NoStream) >>= { first =>
        write(sndExpectedState).map { second =>
          first.streamPosition shouldEqual StreamPosition.Start
          second.streamPosition
        }
      }
    }

    // works with correct expected stream state
    val t1 = run(StreamPosition.Start).map(_ shouldEqual StreamPosition.exact(1))

    // works with any expected stream state
    val t2 = run(StreamState.Any).map(_ shouldEqual StreamPosition.exact(1))

    // works with stream exists expected stream state
    val t3 = run(StreamState.StreamExists).map(_ shouldEqual StreamPosition.exact(1))

    // raises with incorrect expected stream state
    val t4 = run(StreamPosition.exact(1L)).attempt.map {
      case Left(wes: WrongExpectedState) =>
        wes.expected shouldEqual StreamPosition.exact(1L)
        wes.actual shouldEqual StreamPosition.Start
      case r => fail(s"dit not expect $r")
    }

    t1 >> t2 >> t3 >> t4

  }

  test("append to stream with multiple events and stream exists expected stream state ") {

    val id      = genStreamId(s"${streamPrefix}multiple_events_and_stream_exists_")
    val events  = genEvents(5)
    val writes  = events.toList.map(e => streams.appendToStream(id, StreamState.Any, Nel.one(e)))
    val prepare = Stream.eval(writes.sequence).compile.drain

    prepare >> streams
      .appendToStream(id, StreamState.StreamExists, genEvents(1))
      .map(_.streamPosition shouldEqual StreamPosition.exact(5))

  }

  test("append to stream with stream exists expected version works if metadata stream exists") {

    val id    = genStreamId(s"${streamPrefix}stream_exists_and_metadata_stream_exists_")
    val meta  = metaStreams.setMaxAge(id, StreamState.NoStream, 10.seconds)
    val write = streams.appendToStream(id, StreamState.StreamExists, genEvents(1))

    meta >> write.map(_.streamPosition shouldEqual StreamPosition.Start)

  }

  test("append to deleted stream") {

    def run(expectedState: StreamState) = {

      val st = mkSnakeCase(expectedState.render)
      val id = genStreamId(s"${streamPrefix}stream_exists_and_deleted_${st}_")

      streams.delete(id, StreamState.NoStream) >>
        streams.appendToStream(id, expectedState, genEvents(1))
    }

    // with stream exists expected version raises
    run(StreamState.StreamExists).attempt.map {
      case Left(StreamDeleted(_)) => ()
      case r                      => fail(s"dit not expect $r")
    }

  }

  test("can append multiple events at once") {

    val id       = genStreamId(s"${streamPrefix}multiple_events_at_once_")
    val events   = genEvents(100)
    val expected = StreamPosition.exact(99)

    streams.appendToStream(id, StreamState.NoStream, events).map(_.streamPosition shouldEqual expected)
  }

  test("append events with size") {

    val max = 1024 * 1024 // Default ESDB setting

    def mkEvent(sizeBytes: Int): IO[EventData] = IO(UUID.randomUUID()).map { uuid =>

      val et       = EventType("et").unsafe
      val data     = ByteVector.fill(sizeBytes.toLong)(0)
      val metadata = ByteVector.empty
      val ct       = ContentType.Binary

      EventData(et, uuid, data, metadata, ct)
    }

    // less than or equal max append size works
    val t1: IO[WriteResult] = {

      val id       = genStreamId(s"${streamPrefix}append_size_less_or_equal_bytes_")
      val equal    = List(mkEvent(max / 2), mkEvent(max / 2)).sequence.map(Nel.fromListUnsafe)
      val lessThan = List(mkEvent(max / 4), mkEvent(max / 2)).sequence.map(Nel.fromListUnsafe)

      def run(st: StreamState)(data: Nel[EventData]) =
        streams.appendToStream(id, st, data)

      (equal >>= run(StreamState.NoStream)) >> (lessThan >>= run(StreamPosition.exact(1)))

    }

    // greater than max append size raises
    val t2: IO[Unit] = {

      val id          = genStreamId(s"${streamPrefix}append_size_exceeds_bytes_")
      val greaterThan = List(mkEvent(max / 2), mkEvent(max / 2), mkEvent(max + 1)).sequence.map(Nel.fromListUnsafe)

      greaterThan >>= { events =>
        streams.appendToStream(id, StreamState.NoStream, events).attempt.map {
          _ shouldEqual MaximumAppendSizeExceeded(max.some).asLeft
        }
      }
    }

    t1 >> t2

  }

  test("append to implicitly created streams") {

    /*
     * sequence - events written to stream
     * 0em1 - event number 0 written with expected state -1 (minus 1)
     * 1any - event number 1 written with expected state any
     * S_0em1_1em1_E - START bucket, two events in bucket, END bucket
     *
     *   See: https://github.com/EventStore/EventStore/blob/master/src/EventStore.Core.Tests/ClientAPI/appending_to_implicitly_created_stream.cs
     */

    def mkId = genStreamId(s"${streamPrefix}implicitly_created_stream_")

    // sequence 0em1 1e0 2e1 3e2 4e3 5e4
    val t1 = {

      def run(nextExpected: StreamState) = {

        val id     = mkId
        val events = genEvents(6)

        for {
          _ <- streams.appendToStream(id, StreamState.NoStream, events)
          _ <- streams.appendToStream(id, nextExpected, Nel.one(events.head))
          e <- streams.readStreamForwards(id, maxCount = events.size + 1L).compile.toList

        } yield e.size shouldEqual events.size
      }

      // 0em1 is idempotent
      val r1 = run(StreamState.NoStream)

      // 0any is idempotent
      val r2 = run(StreamState.Any)

      r1 >> r2

    }

    // sequence 0em1 1e0 2e1 3e2 4e3 5e4 0e5 is non idempotent
    val t2 = {

      val id     = mkId
      val events = genEvents(6)

      for {
        _ <- streams.appendToStream(id, StreamState.NoStream, events)
        _ <- streams.appendToStream(id, StreamPosition.exact(5), Nel.one(events.head))
        e <- streams.readStreamForwards(id, maxCount = events.size + 2L).compile.toList

      } yield e.size shouldEqual events.size + 1

    }

    // sequence 0em1 1e0 2e1 3e2 4e3 5e4 0e6 raises
    val t3 = {

      val id     = mkId
      val events = genEvents(6)
      val result = for {
        _ <- streams.appendToStream(id, StreamState.NoStream, events)
        _ <- streams.appendToStream(id, StreamPosition.exact(6), Nel.one(events.head))
      } yield ()

      result.attempt.map {
        _ shouldEqual WrongExpectedState(id, StreamPosition.exact(6), StreamPosition.exact(5)).asLeft
      }
    }

    // sequence 0em1 1e0 2e1 3e2 4e3 5e4 0e4 raises
    val t4 = {

      val id     = mkId
      val events = genEvents(6)

      val result = for {
        _ <- streams.appendToStream(id, StreamState.NoStream, events)
        _ <- streams.appendToStream(id, StreamPosition.exact(4), Nel.one(events.head))
      } yield ()

      result.attempt.map {
        _ shouldEqual WrongExpectedState(id, StreamPosition.exact(4), StreamPosition.exact(5)).asLeft
      }
    }

    // sequence 0em1 0e0 is non idempotent
    val t5 = {

      val id     = mkId
      val events = genEvents(1)

      for {
        _ <- streams.appendToStream(id, StreamState.NoStream, events)
        _ <- streams.appendToStream(id, StreamPosition.Start, Nel.one(events.head))
        e <- streams.readStreamForwards(id, maxCount = events.size + 2L).compile.toList

      } yield e.size shouldEqual events.size + 1
    }

    // sequence 0em1
    val t6 = {

      def run(nextExpected: StreamState) = {

        val id     = mkId
        val events = genEvents(1)

        for {
          _ <- streams.appendToStream(id, StreamState.NoStream, events)
          _ <- streams.appendToStream(id, nextExpected, Nel.one(events.head))
          e <- streams.readStreamForwards(id, maxCount = events.size + 1L).compile.toList

        } yield e.size shouldEqual events.size
      }

      // 0em1 is idempotent
      val r1 = run(StreamState.NoStream)

      // 0any is idempotent
      val r2 = run(StreamState.Any)

      r1 >> r2
    }

    // sequence 0em1 1e0 2e1 1any 1any is idempotent
    val t7 = {

      val id = mkId
      val e1 = genEvent
      val e2 = genEvent
      val e3 = genEvent

      for {
        _ <- streams.appendToStream(id, StreamState.NoStream, Nel.of(e1, e2, e3))
        _ <- streams.appendToStream(id, StreamState.Any, Nel.of(e2))
        _ <- streams.appendToStream(id, StreamState.Any, Nel.of(e2))
        e <- streams.readStreamForwards(id, maxCount = 4).compile.toList

      } yield e.size shouldEqual 3
    }

    // sequence S 0em1 1em1 E
    val t8 = {

      def run(nextState: StreamState, onlyLast: Boolean) = {

        val id         = mkId
        val e1         = genEvent
        val e2         = genEvent
        val events     = Nel.of(e1, e2)
        val nextEvents = if (onlyLast) Nel.one(e2) else events

        for {
          _ <- streams.appendToStream(id, StreamState.NoStream, events)
          _ <- streams.appendToStream(id, nextState, nextEvents)
          e <- streams.readStreamForwards(id, maxCount = events.size + 1L).compile.toList

        } yield e.size shouldEqual events.size
      }

      // S 0em1 E is idempotent
      val r1 = run(StreamState.NoStream, onlyLast = false)

      // S 0any E is idempotent
      val r2 = run(StreamState.Any, onlyLast = false)

      // S 1e0  E is idempotent
      val r3 = run(StreamPosition.Start, onlyLast = true)

      // S 1any E is idempotent
      val r4 = run(StreamState.Any, onlyLast = true)

      r1 >> r2 >> r3 >> r4
    }

    // sequence S 0em1 1em1 E S 0em1 1em1 2em1 E raises
    val t9 = {

      val id     = mkId
      val e1     = genEvent
      val e2     = genEvent
      val e3     = genEvent
      val first  = Nel.of(e1, e2)
      val second = Nel.of(e1, e2, e3)

      streams.appendToStream(id, StreamState.NoStream, first) >> {
        streams.appendToStream(id, StreamState.NoStream, second).attempt.map {
          _ shouldEqual WrongExpectedState(id, StreamState.NoStream, StreamPosition.exact(1L)).asLeft
        }
      }
    }

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9

  }

}

class DeleteSuite extends SnSpec {

  val streamPrefix = s"streams_delete_${genIdentifier}_"

  test("delete a stream that does not exist") {

    def run(expectedState: StreamState) = {

      val st = mkSnakeCase(expectedState.render)
      val id = genStreamId(s"${streamPrefix}non_existing_stream_with_expected_state${st}_")

      streams.delete(id, expectedState).void
    }

    // works with no stream expected stream state
    val r1 = run(StreamState.NoStream)

    // works with any expected stream state
    val r2 = run(StreamState.Any)

    // raises with wrong expected stream state
    val r3 = run(StreamPosition.Start).attempt.map {
      case Left(WrongExpectedState(_, StreamPosition.Start, StreamState.NoStream)) => ()
      case r                                                                       => fail(s"dit not expect $r")
    }

    r1 >> r2 >> r3

  }

  test("a stream should return log position") {

    val id     = genStreamId(s"${streamPrefix}return_log_position_")
    val events = genEvents(1)

    for {
      wr  <- streams.appendToStream(id, StreamState.NoStream, events)
      pos <- streams.readAllBackwards(maxCount = 1).compile.lastOrError.map(_.logPosition)
      dr  <- streams.delete(id, wr.streamPosition)

    } yield assert(dr.logPosition > pos)

  }

  test("a stream and reading raises") {

    val id     = genStreamId(s"${streamPrefix}reading_raises_")
    val events = genEvents(1)

    for {
      wr <- streams.appendToStream(id, StreamState.NoStream, events)
      _  <- streams.delete(id, wr.streamPosition)
      at <- streams.readStreamForwards(id, maxCount = 1).compile.drain.attempt

    } yield
      at match {
        case Left(StreamNotFound(_)) => ()
        case r                       => fail(s"dit not expect $r")
      }

  }

  test("a stream and tombstone works as expected") {

    val id     = genStreamId(s"${streamPrefix}and_tombstone_it_")
    val events = genEvents(2)

    for {
      wr  <- streams.appendToStream(id, StreamState.NoStream, events)
      _   <- streams.delete(id, wr.streamPosition)
      _   <- streams.tombstone(id, StreamState.Any)
      rat <- streams.readStreamForwards(id, maxCount = 2).compile.drain.attempt
      mat <- metaStreams.getMetadata(id).attempt
      aat <- streams.appendToStream(id, StreamState.Any, genEvents(1)).attempt

    } yield {
      rat match {
        case Left(e: StreamDeleted) => e.streamId shouldEqual id.stringValue
        case r                      => fail(s"dit not expect $r")
      }
      mat match {
        case Left(e: StreamDeleted) => e.streamId shouldEqual id.metaId.stringValue
        case r                      => fail(s"dit not expect $r")
      }
      aat match {
        case Left(e: StreamDeleted) => e.streamId shouldEqual id.stringValue
        case r                      => fail(s"dit not expect $r")
      }
    }

  }

  test("a stream and recreate with") {

    def run(expectedState: StreamState) = {

      val st           = mkSnakeCase(expectedState.render)
      val id           = genStreamId(s"${streamPrefix}and_recreate_with_expected_state_${st}_")
      val beforeEvents = genEvents(1)
      val afterEvents  = genEvents(3)

      for {
        wr1 <- streams.appendToStream(id, StreamState.NoStream, beforeEvents)
        _   <- streams.delete(id, wr1.streamPosition)
        wr2 <- streams.appendToStream(id, expectedState, afterEvents)
        _   <- IO.sleep(100.millis) // Workaround for ES github issue #1744
        evt <- streams.readStreamForwards(id, maxCount = 3).compile.toList
        tbm <- metaStreams.getTruncateBefore(id)

      } yield {

        wr1.streamPosition shouldEqual StreamPosition.Start
        wr2.streamPosition shouldEqual StreamPosition.exact(3)

        evt.size shouldEqual 3
        evt.map(_.eventData).toNel shouldEqual afterEvents.some
        evt.map(_.streamPosition) shouldEqual List(StreamPosition.exact(1),
                                                   StreamPosition.exact(2),
                                                   StreamPosition.exact(3))

        tbm shouldEqual MetaStreams.Result(StreamPosition.exact(1), StreamPosition.exact(1).some).some

      }

    }

    // no stream expected stream state
    val r1 = run(StreamState.NoStream)

    // any expected stream state
    val r2 = run(StreamState.Any)

    // exact expected stream state
    val r3 = run(StreamPosition.Start)

    r1 >> r2 >> r3

  }

  test("a stream and recreate preserves metadata except truncate before") {

    val id           = genStreamId(s"${streamPrefix}and_recreate_preserves_metadata_")
    val beforeEvents = genEvents(2)
    val afterEvents  = genEvents(3)

    val metadata = StreamMetadata.empty
      .withAcl(StreamAcl.empty.withDeleteRoles(Set("some-role")))
      .withMaxCount(MaxCount(100).unsafe)
      .withTruncateBefore(StreamPosition.exact(Long.MaxValue))
      .withCustom("k1" -> Json.True, "k2" -> Json.fromInt(17), "k3" -> Json.fromString("some value"))

    for {
      swr    <- streams.appendToStream(id, StreamState.NoStream, beforeEvents)
      mwr    <- metaStreams.setMetadata(id, StreamState.NoStream, metadata)
      _      <- streams.appendToStream(id, StreamPosition.exact(1), afterEvents)
      _      <- IO.sleep(1.second) // Workaround for ES github issue #1744
      events <- streams.readStreamForwards(id, maxCount = 3).compile.toList
      meta   <- metaStreams.getMetadata(id)

    } yield {

      swr.streamPosition shouldEqual StreamPosition.exact(1)
      mwr.streamPosition shouldEqual StreamPosition.Start

      events.size shouldEqual afterEvents.size
      events.map(_.streamPosition) shouldEqual List(StreamPosition.exact(2),
                                                    StreamPosition.exact(3),
                                                    StreamPosition.exact(4))

      meta.fold(fail("Did expect metadata result")) { m =>
        m.streamPosition shouldEqual StreamPosition.exact(1)
        m.data shouldEqual metadata.withTruncateBefore(StreamPosition.exact(2))
      }
    }

  }

  test("a stream and recreate raises if not first write") {

    val id = genStreamId(s"${streamPrefix}and_recreate_only_first_write_")

    for {
      wr1 <- streams.appendToStream(id, StreamState.NoStream, genEvents(2))
      _   <- streams.delete(id, wr1.streamPosition)
      wr2 <- streams.appendToStream(id, StreamState.NoStream, genEvents(3))
      wr3 <- streams.appendToStream(id, StreamState.NoStream, genEvents(1)).attempt
    } yield {
      wr1.streamPosition shouldEqual StreamPosition.exact(1)
      wr2.streamPosition shouldEqual StreamPosition.exact(4)
      wr3 shouldEqual WrongExpectedState(id, StreamState.NoStream, StreamPosition.exact(4L)).asLeft
    }

  }

  test("a stream and recreate with multiple appends and expected stream state any") {

    val id      = genStreamId(s"${streamPrefix}and_recreate_multiple_writes_with_any_expected_state_")
    val events0 = genEvents(2)
    val events1 = genEvents(3)
    val events2 = genEvents(2)

    for {
      wr0    <- streams.appendToStream(id, StreamState.NoStream, events0)
      _      <- streams.delete(id, wr0.streamPosition)
      wr1    <- streams.appendToStream(id, StreamState.Any, events1)
      wr2    <- streams.appendToStream(id, StreamState.Any, events2)
      events <- streams.readStreamForwards(id, maxCount = 5).compile.toList
      tbr    <- metaStreams.getTruncateBefore(id)
    } yield {

      wr0.streamPosition shouldEqual StreamPosition.exact(1)
      wr1.streamPosition shouldEqual StreamPosition.exact(4)
      wr2.streamPosition shouldEqual StreamPosition.exact(6)

      events.size shouldEqual 5
      events.map(_.eventData) shouldEqual events1.concatNel(events2).toList
      events.map(_.streamPosition) shouldEqual (2L to 6L).map(StreamPosition.exact).toList

      tbr.fold(fail("Did expect read result")) { result =>
        result.data shouldEqual StreamPosition.exact(2).some
        result.streamPosition shouldEqual StreamPosition.exact(1)
      }
    }

  }

  test("a stream and recreate on empty when metadata set") {

    val id = genStreamId(s"${streamPrefix}and_recreate_on_empty_when_metadata_set_")

    val metadata = StreamMetadata.empty
      .withMaxCount(MaxCount(100).unsafe)
      .withAcl(StreamAcl.empty.withDeleteRoles(Set("some-role")))
      .withTruncateBefore(StreamPosition.exact(Long.MaxValue))
      .withCustom("k1" -> Json.True, "k2" -> Json.fromInt(17), "k3" -> Json.fromString("some value"))

    for {
      _    <- streams.delete(id, StreamState.NoStream)
      mw   <- metaStreams.setMetadata(id, StreamPosition.Start, metadata)
      read <- streams.readStreamForwards(id, maxCount = 1).compile.toList.attempt
      meta <- metaStreams.getMetadata(id)
    } yield {

      mw.streamPosition shouldEqual StreamPosition.exact(1)

      read match {
        case Left(e: StreamNotFound) => e.streamId shouldEqual id.stringValue
        case r                       => fail(s"dit not expect $r")
      }

      meta.fold(fail("Did expect read result")) { m =>
        m.streamPosition shouldEqual StreamPosition.exact(2)
        m.data shouldEqual metadata.withTruncateBefore(StreamPosition.Start)
      }
    }

  }

  test("a stream and recreate on non-empty when metadata set") {

    val id     = genStreamId(s"${streamPrefix}and_recreate_on_non_empty_when_metadata_set_")
    val events = genEvents(2)

    val metadata = StreamMetadata.empty
      .withMaxCount(MaxCount(100).unsafe)
      .withAcl(StreamAcl.empty.withDeleteRoles(Set("some-role")))
      .withCustom("k1" -> Json.True, "k2" -> Json.fromInt(17), "k3" -> Json.fromString("some value"))

    for {
      sw1  <- streams.appendToStream(id, StreamState.NoStream, events)
      _    <- streams.delete(id, sw1.streamPosition)
      mw1  <- metaStreams.setMetadata(id, StreamPosition.Start, metadata)
      read <- streams.readStreamForwards(id, maxCount = 2).compile.toList
      meta <- metaStreams.getMetadata(id)
    } yield {

      sw1.streamPosition shouldEqual StreamPosition.exact(1)
      mw1.streamPosition shouldEqual StreamPosition.exact(1)
      read.size shouldEqual 0

      meta.fold(fail("Did expect read result")) { m =>
        m.data shouldEqual metadata.withTruncateBefore(StreamPosition.exact(events.size.toLong))
      }
    }

  }

}

class TombstoneSuite extends SnSpec {

  val streamPrefix = s"streams_tombstone_${genIdentifier}_"

  test("a stream that does not exist") {

    def run(expectedState: StreamState) = {

      val st = mkSnakeCase(expectedState.render)
      val id = genStreamId(s"${streamPrefix}non_existing_stream_with_expected_state_${st}_")

      streams.tombstone(id, expectedState).void
    }

    // works with no stream expected stream state
    val r1 = run(StreamState.NoStream)

    // works with any expected stream state
    val r2 = run(StreamState.Any)

    // raises with wrong expected stream state
    val r3 = run(StreamPosition.Start).attempt.map {
      case Left(wes: WrongExpectedState) =>
        wes.expected shouldEqual StreamPosition.Start
        wes.actual shouldEqual StreamState.NoStream
      case r => fail(s"dit not expect $r")
    }

    r1 >> r2 >> r3

  }

  test("a stream should return log position") {

    val id     = genStreamId(s"${streamPrefix}return_log_position_")
    val events = genEvents(1)

    for {
      wr  <- streams.appendToStream(id, StreamState.NoStream, events)
      pos <- streams.readAllBackwards(maxCount = 1).compile.lastOrError.map(_.logPosition)
      tr  <- streams.tombstone(id, wr.streamPosition)

    } yield assert(tr.logPosition > pos)

  }

  test("a tombstoned stream should raise") {
    val id = genStreamId(s"${streamPrefix}tombstoned_stream_")
    streams.tombstone(id, StreamState.NoStream) >> {
      streams.tombstone(id, StreamState.NoStream).attempt.map {
        case Left(StreamDeleted(_)) => ()
        case r                      => fail(s"dit not expect $r")
      }
    }
  }

}

class SubcribeToAllSuite extends SnSpec {

  val streamPrefix                       = s"streams_subscribe_to_all_${genIdentifier}_"
  val fromBeginning: Option[LogPosition] = Option.empty
  val fromEnd: Option[LogPosition]       = LogPosition.End.some

  test("works when streams do not exist prior to subscribing") {

    def mkId(suffix: String): StreamId =
      genStreamId(s"${streamPrefix}non_existing_stream_${suffix}_")

    def write(id: StreamId, data: Nel[EventData]) =
      Stream.eval(streams.appendToStream(id, StreamState.NoStream, data))

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
        events.size.toLong shouldEqual count
        events.filter(_.streamId == s1).map(_.eventData).toNel shouldEqual s1Events.some
        events.filter(_.streamId == s2).map(_.eventData).toNel shouldEqual s2Events.some
      }

    }

    // from beginning
    val r1 = run(fromBeginning)

    // from log position
    val r2 = {
      val initId  = mkId("init")
      val prepare = write(initId, genEvents(10)).map(_.logPosition)
      prepare.compile.lastOrError >>= { pos => run(pos.some) }
    }

    // from end
    val r3 = run(fromEnd)

    r1 >> r2 >> r3

  }

  test("works when streams exist prior to subscribing") {

    def mkId(suffix: String): StreamId =
      genStreamId(s"${streamPrefix}existing_stream_${suffix}_")

    def write(id: StreamId, data: Nel[EventData], st: StreamState = StreamState.NoStream) =
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
      (write(s1, s1After, r1) >> write(s2, s2After, r2)).delayBy(500.millis)

    def run(exclusiveFrom: Option[LogPosition], s1: StreamId, s2: StreamId) =
      writeBefore(s1, s2) >>= { case (wa, wb) =>
        subscribe(exclusiveFrom, Set(s1, s2).contains)
          .concurrently(writeAfter(s1, wa.streamPosition, s2, wb.streamPosition))
          .map(_.eventData)
      }

    // from beginning
    val r1 = {

      val s1    = mkId("s1_begin")
      val s2    = mkId("s2_begin")
      val count = all.size.toLong

      run(fromBeginning, s1, s2).take(count).compile.toList.map(_.toNel shouldEqual all.some)
    }

    // from log position
    val r2 = {

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

      result.compile.toList.map(_.toNel shouldEqual expected.some)
    }

    // from end
    val r3 = {

      val s1    = mkId("s1_end")
      val s2    = mkId("s2_end")
      val count = after.size.toLong

      run(fromEnd, s1, s2).take(count).compile.toList.map(_.toNel shouldEqual after.some)
    }

    r1 >> r2 >> r3

  }

}

class SubcribeToAllFilteredSuite extends SnSpec {

  import EventFilter._
  import StreamState.NoStream

  val maxSearchWindow = 32
  val multiplier      = 3

  def writeRandom(amount: Int): IO[WriteResult] = genStreamUuid[IO] >>= { sid =>
    streams.appendToStream(sid, NoStream, genEvents(amount))
  }

  def testBeginningOrEnd(from: Option[LogPosition.End.type], includeBefore: Boolean)(
    prefix: String,
    filter: EventFilter,
    adjustFn: Endo[EventData]
  ): IO[Unit] = {

    val options  = SubscriptionFilterOptions(filter, maxSearchWindow.some, multiplier)
    val before   = genEvents(10).map(adjustFn)
    val after    = genEvents(10).map(adjustFn)
    val expected = from.fold(includeBefore.fold(before.concatNel(after), after))(_ => after)

    def mkStreamId = genStreamId(s"${prefix}_")

    def write(eds: Nel[EventData]) =
      eds.traverse(e => streams.appendToStream(mkStreamId, NoStream, Nel.one(e)))

    val subscribe = streams.subscribeToAll(from, options).takeThrough(_.fold(_ => true, _.eventData != after.last))

    val writeBefore = write(before).whenA(includeBefore).void
    val writeAfter  = write(after).void

    val result = for {
      _    <- Stream.eval(writeRandom(30))
      _    <- Stream.eval(writeBefore)
      _    <- Stream.eval(writeRandom(30))
      _    <- Stream.sleep[IO](500.millis)
      data <- subscribe.concurrently(Stream.eval(writeAfter).delayBy(500.millis))
    } yield data

    result.compile.toList.map { r =>
      val (c, e) = r.partitionMap(identity)
      e.size shouldEqual expected.size
      e.map(_.eventData).toNel shouldEqual expected.some
      c.nonEmpty shouldEqual true
    }
  }

  ///

  def testPosition(includeBefore: Boolean)(
    prefix: String,
    filter: EventFilter,
    adjustFn: Endo[EventData]
  ): IO[Unit] = {

    val options  = SubscriptionFilterOptions(filter, maxSearchWindow.some, multiplier)
    val before   = genEvents(10).map(adjustFn)
    val after    = genEvents(10).map(adjustFn)
    val expected = includeBefore.fold(before.concatNel(after), after)
    val existing = includeBefore.fold("existing_stream", "non_existing_stream")

    def mkStreamId =
      genStreamId(s"${prefix}_${existing}_from_log_position")

    def write(eds: Nel[EventData]) =
      eds.traverse(e => streams.appendToStream(mkStreamId, NoStream, Nel.one(e)))

    def subscribe(from: LogPosition) =
      streams.subscribeToAll(from.some, options).takeThrough(_.fold(_ => true, _.eventData != after.last))

    val writeBefore = write(before).whenA(includeBefore).void
    val writeAfter  = write(after).void

    val result = for {
      pos  <- Stream.eval(writeRandom(500)).map(_.logPosition)
      _    <- Stream.eval(writeBefore)
      _    <- Stream.eval(writeRandom(500))
      _    <- Stream.sleep[IO](500.millis)
      data <- subscribe(pos).concurrently(Stream.eval(writeAfter).delayBy(500.millis))
    } yield data

    result.compile.toList.map { r =>
      val (c, e) = r.partitionMap(identity)
      e.size shouldEqual expected.size
      e.map(_.eventData).toNel shouldEqual expected.some
      c.nonEmpty shouldEqual true
    }
  }

  ///

  val replaceType: String => Endo[EventData] =
    et => ed => EventData(et, ed.eventId, ed.data, ed.metadata, ed.contentType).unsafe

  val mkPrefix: String => String =
    p => s"streams_subscribe_to_all_filter_${p}_$genIdentifier"

  def runBeginningAndEnd(includeBefore: Boolean): IO[Unit] = {

    val existing = includeBefore.fold("exists", "non_present")

    val fromBeginning = {

      val append = s"_beginning_$existing"

      val siPrefix = mkPrefix(s"stream_id_prefix$append")
      val siRegex  = mkPrefix(s"stream_id_regex$append")
      val etPrefix = mkPrefix(s"event_type_prefix$append")
      val etRegex  = mkPrefix(s"event_type_regex_$append")

      val testBeginning = testBeginningOrEnd(None, includeBefore) _

      val r1 = testBeginning(siPrefix, streamIdPrefix(siPrefix), identity)
      val r2 = testBeginning(siRegex, streamIdRegex(siRegex), identity)
      val r3 = testBeginning(etPrefix, eventTypePrefix(etPrefix), replaceType(etPrefix))
      val r4 = testBeginning(etRegex, eventTypeRegex(etRegex), replaceType(etRegex))

      r1 >> r2 >> r3 >> r4
    }

    val fromEnd = {

      val append   = s"_end_$existing"
      val siPrefix = mkPrefix(s"stream_id_prefix$append")
      val siRegex  = mkPrefix(s"stream_id_regex$append")
      val etPrefix = mkPrefix(s"event_type_prefix$append")
      val etRegex  = mkPrefix(s"event_type_regex_$append")

      val testEnd = testBeginningOrEnd(LogPosition.End.some, includeBefore) _

      val r1 = testEnd(siPrefix, streamIdPrefix(siPrefix), identity)
      val r2 = testEnd(siRegex, streamIdRegex(siRegex), identity)
      val r3 = testEnd(etPrefix, eventTypePrefix(etPrefix), replaceType(etPrefix))
      val r4 = testEnd(etRegex, eventTypeRegex(etRegex), replaceType(etRegex))

      r1 >> r2 >> r3 >> r4
    }

    fromBeginning >> fromEnd

  }

  def runPosition(includeBefore: Boolean): IO[Unit] = {

    val existing = includeBefore.fold("exists", "non_present")

    val append   = s"_log_position_$existing"
    val siPrefix = mkPrefix(s"stream_id_prefix$append")
    val siRegex  = mkPrefix(s"stream_id_regex$append")
    val etPrefix = mkPrefix(s"event_type_prefix$append")
    val etRegex  = mkPrefix(s"event_type_regex_$append")

    val test = testPosition(includeBefore) _

    val r1 = test(siPrefix, streamIdPrefix(siPrefix), identity)
    val r2 = test(siRegex, streamIdRegex(siRegex), identity)
    val r3 = test(etPrefix, eventTypePrefix(etPrefix), replaceType(etPrefix))
    val r4 = test(etRegex, eventTypeRegex(etRegex), replaceType(etRegex))

    r1 >> r2 >> r3 >> r4

  }

  test("works when streams does exist not prior to subscribing") {
    runBeginningAndEnd(includeBefore = false) >> runPosition(includeBefore = false)
  }

  test("works when streams exist prior to subscribing") {
    runBeginningAndEnd(includeBefore = true) >> runPosition(includeBefore = true)
  }

}

class SubcribeToStreamSuite extends SnSpec {

  val streamPrefix: String                               = s"streams_subscribe_to_stream_${genIdentifier}_"
  val fromBeginning: Option[StreamPosition]              = Option.empty
  val fromStreamPosition: Long => Option[StreamPosition] = r => StreamPosition.exact(r).some
  val fromEnd: Option[StreamPosition]                    = StreamPosition.End.some

  test("works when stream does not exist prior to subscribing") {

    val events = genEvents(50)

    def test(exclusivefrom: Option[StreamPosition], takeCount: Int) = {

      val id        = genStreamId(s"${streamPrefix}non_existing_stream_")
      val subscribe = streams.subscribeToStream(id, exclusivefrom).take(takeCount.toLong).map(_.eventData)
      val write     = Stream.eval(streams.appendToStream(id, StreamState.NoStream, events)).delayBy(500.millis)
      val result    = subscribe.concurrently(write)

      result.compile.toList
    }

    val r1 = test(fromBeginning, events.size).map(_ shouldEqual events.toList)
    val r2 = test(fromStreamPosition(4), events.size - 5).map(_ shouldEqual events.toList.drop(5))
    val r3 = test(fromEnd, events.size).map(_ shouldEqual events.toList)

    r1 >> r2 >> r3

  }

  test("works with multiple subscriptions to same stream") {

    val eventCount      = 10
    val subscriberCount = 4
    val events          = genEvents(eventCount)

    def test(exclusivFrom: Option[StreamPosition], takeCount: Int) = {

      val id    = genStreamId(s"${streamPrefix}multiple_subscriptions_to_same_stream_")
      val write = Stream.eval(streams.appendToStream(id, StreamState.NoStream, events)).delayBy(500.millis)

      def mkSubscribers(onEvent: IO[Unit]): Stream[IO, StreamEvent] = Stream
        .emit(streams.subscribeToStream(id, exclusivFrom).evalTap(_ => onEvent).take(takeCount.toLong))
        .repeat
        .take(subscriberCount.toLong)
        .parJoin(subscriberCount)

      val result: Stream[IO, Int] = Stream.eval(Ref.of[IO, Int](0)) >>= { ref =>
        Stream(mkSubscribers(ref.update(_ + 1)), write).parJoinUnbounded >> Stream.eval(ref.get)
      }

      result.compile.lastOrError.map(_.shouldEqual(takeCount * subscriberCount))
    }

    val r1 = test(fromBeginning, eventCount)
    val r2 = test(fromStreamPosition(0), eventCount - 1)
    val r3 = test(fromEnd, eventCount)

    r1 >> r2 >> r3

  }

  test("works with existing stream") {

    val beforeEvents = genEvents(40)
    val afterEvents  = genEvents(10)
    val totalEvents  = beforeEvents.concatNel(afterEvents)

    def test(exclusiveFrom: Option[StreamPosition], takeCount: Int) = {

      val id = genStreamId(s"${streamPrefix}existing_and_new_")

      val beforeWrite =
        Stream.eval(streams.appendToStream(id, StreamState.NoStream, beforeEvents))

      def afterWrite(st: StreamState): Stream[IO, WriteResult] =
        Stream.eval(streams.appendToStream(id, st, afterEvents)).delayBy(500.millis)

      def subscribe(onEvent: StreamEvent => IO[Unit]): Stream[IO, StreamEvent] =
        streams.subscribeToStream(id, exclusiveFrom).evalTap(onEvent).take(takeCount.toLong)

      val result: Stream[IO, List[EventData]] = for {
        ref        <- Stream.eval(Ref.of[IO, List[EventData]](Nil))
        st         <- beforeWrite.map(_.streamPosition)
        _          <- Stream.sleep[IO](500.millis)
        _          <- subscribe(e => ref.update(_ :+ e.eventData)).concurrently(afterWrite(st))
        readEvents <- Stream.eval(ref.get)
      } yield readEvents

      result.compile.lastOrError

    }

    // from beginning - reads all events and listens for new ones
    val r1 = test(fromBeginning, totalEvents.size).map(_.toNel shouldEqual totalEvents.some)

    // from stream position - reads events after stream position and listens for new ones
    val r2 = test(fromStreamPosition(29), 20).map(_ shouldEqual totalEvents.toList.drop(30))

    // from end - listens for new events at given end of stream
    val r3 = test(fromEnd, afterEvents.size).map(_.toNel shouldEqual afterEvents.some)

    r1 >> r2 >> r3

  }

  test("raises when stream is tombstoned") {

    def test(exclusiveFrom: Option[StreamPosition]) = {

      val id        = genStreamId(s"${streamPrefix}stream_is_tombstoned_")
      val subscribe = streams.subscribeToStream(id, exclusiveFrom)
      val delete    = Stream.eval(streams.tombstone(id, StreamState.Any)).delayBy(500.millis)
      val expected  = StreamDeleted(id.stringValue).asLeft

      subscribe.concurrently(delete).compile.last.attempt.map(_.shouldEqual(expected))
    }

    val r1 = test(fromBeginning)
    val r2 = test(fromStreamPosition(5))
    val r3 = test(fromEnd)

    r1 >> r2 >> r3

  }

}
