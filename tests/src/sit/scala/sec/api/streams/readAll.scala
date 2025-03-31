/*
 * Copyright 2020 Scala Event Sourcing client for KurrentDB
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

import java.util as ju
import scodec.bits.ByteVector
import cats.data.NonEmptyList as Nel
import cats.syntax.all.*
import cats.effect.IO
import sec.syntax.all.*
import sec.api.exceptions.*
import sec.helpers.implicits.*

class ReadAllSuite extends SnSuite:

  import StreamState.*
  import Direction.*
  import LogPosition.*

  val streamPrefix    = s"streams_read_all_${genIdentifier}_"
  val eventTypePrefix = s"sec.$genIdentifier.Event"

  val id1     = genStreamId(streamPrefix)
  val id2     = genStreamId(streamPrefix)
  val events1 = genEvents(50, eventTypePrefix)
  val events2 = genEvents(50, eventTypePrefix)
  val written = events1 ::: events2

  def read(from: LogPosition, direction: Direction, maxCount: Long = 1) =
    streams.readAll(from, direction, maxCount, resolveLinkTos = false).compile.toList

  //

  test("init") {
    streams.appendToStream(id1, NoStream, events1) *>
      streams.appendToStream(id2, NoStream, events2)
  }

  //

  group("forwards") {

    test("reading from start yields events") {
      assertIO(read(Start, Forwards, written.size.toLong).map(_.size), written.size)
    }

    test("reading from end yields no events") {
      assertIOBoolean(read(End, Forwards).map(_.isEmpty))
    }

    test("events are in same order as written") {
      assertIO(
        read(Start, Forwards, Int.MaxValue - 1).map(_.collect { case e if e.streamId == id1 => e.eventData }),
        events1.toList
      )
    }

    test("max count <= 0 yields no events") {
      assertIOBoolean(read(Start, Forwards, 0).map(_.isEmpty))
    }

    test("max count is respected") {
      assertIO(
        streams
          .readAll(Start, Forwards, events1.length / 2L, resolveLinkTos = false)
          .take(events1.length.toLong)
          .compile
          .toList
          .map(_.size),
        events1.length / 2
      )
    }

    def deleted(normalDelete: Boolean) = {

      val suffix = if (normalDelete) "" else "tombstoned"
      val id     = genStreamId(s"streams_read_all_deleted_${suffix}_")
      val events = genEvents(10)
      val write  = streams.appendToStream(id, NoStream, events)

      def delete(er: StreamPosition.Exact) =
        if (normalDelete) streams.delete(id, er) else streams.tombstone(id, er)

      val decodeJson: EventData => ErrorOr[StreamMetadata] =
        _.data.decodeUtf8.flatMap(io.circe.parser.decode[StreamMetadata](_))

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

      def verify(es: List[Event]): Unit =
        es.lastOption.fold(fail("expected metadata")) { ts =>
          if (normalDelete) {
            assertEquals(es.dropRight(1).map(_.eventData), events.toList)
            assertEquals(ts.streamId, id.metaId)
            assertEquals(ts.eventData.eventType, EventType.StreamMetadata)
            assertEquals(
              decodeJson(ts.record.eventData).map(_.state.truncateBefore.map(_.value)),
              ULong(Long.MaxValue).some.asRight
            )
          } else {
            assertEquals(es.dropRight(1).map(_.eventData), events.toList)
            assertEquals(ts.streamId, id)
            assertEquals(ts.eventData.eventType, EventType.StreamDeleted)
          }
        }

      setup.map(verify)

    }

    test("deleted stream")(deleted(normalDelete = true))
    test("tombstoned stream")(deleted(normalDelete = false))

    test("max count deleted events are not resolved") {

      val deletedId               = genStreamId("streams_read_all_linkto_deleted_")
      val linkId                  = genStreamId("streams_read_all_linkto_link_")
      def encode(content: String) = ByteVector.encodeUtf8(content).unsafeGet

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
        streams.appendToStream(id, Any, data)

      def readLink(resolve: Boolean) =
        streams.readStreamForwards(linkId, maxCount = 3, resolveLinkTos = resolve).map(_.eventData).compile.toList

      val e1 = genEvents(1)
      val e2 = genEvents(1)
      val e3 = genEvents(1)

      val l1 = linkData(0)
      val l2 = linkData(1)
      val l3 = linkData(2)

      append(deletedId, e1) >>
        metaStreams.setMaxCount(deletedId, NoStream, 2) >>
        append(deletedId, e2) >>
        append(deletedId, e3) >>
        append(linkId, l1) >>
        append(linkId, l2) >>
        append(linkId, l3) >>
        assertIO(readLink(resolve = true), (e2 ::: e3).toList) >>
        assertIO(readLink(resolve = false), (l1 ::: l2 ::: l3).toList)
    }

  }

  group("backwards") {

    test("reading from start yields no events") {
      assertIOBoolean(read(Start, Backwards).map(_.isEmpty))
    }

    test("reading from end yields events") {
      assertIOBoolean(read(End, Backwards).map(_.lastOption.nonEmpty))
    }

    test("events are in reverse order as written") {
      assertIO(
        read(End, Backwards, Int.MaxValue - 1)
          .map(_.collect { case e if e.streamId == id1 => e.eventData })
          .map(_.reverse),
        events1.toList
      )
    }

    test("max count <= 0 yields no events") {
      assertIOBoolean(read(End, Backwards, 0).map(_.isEmpty))
    }

    test("max count is respected") {
      assertIO(
        streams
          .readAll(End, Backwards, events1.length / 2L, resolveLinkTos = false)
          .take(events1.length.toLong)
          .compile
          .toList
          .map(_.size),
        events1.length / 2
      )
    }

  }
