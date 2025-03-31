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

import cats.syntax.all.*
import sec.api.exceptions.*

class ReadStreamSuite extends SnSuite:

  import StreamState.*
  import StreamPosition.*
  import Direction.*

  val streamPrefix = s"streams_read_stream_${genIdentifier}_"
  val id           = genStreamId(streamPrefix)
  val events       = genEvents(25)

  def read(id: StreamId, from: StreamPosition, direction: Direction, count: Long = 50) =
    streams.readStream(id, from, direction, count, resolveLinkTos = false).compile.toList

  def readData(id: StreamId, from: StreamPosition, direction: Direction, count: Long = 50) =
    read(id, from, direction, count).map(_.map(_.eventData))

  //

  test("init")(streams.appendToStream(id, NoStream, events))

  //

  group("read stream forwards") {

    test("reading from start yields events") {
      assertIO(read(id, Start, Forwards).map(_.size), events.size)
    }

    test("reading from start with count <= 0 yields no events") {
      assertIOBoolean(read(id, Start, Forwards, -1L).map(_.isEmpty)) *>
        assertIOBoolean(read(id, Start, Forwards, 0).map(_.isEmpty))
    }

    test("reading from end yields no events") {
      assertIOBoolean(read(id, End, Forwards).map(_.isEmpty))
    }

    test("events are in same order as written") {
      assertIO(readData(id, Start, Forwards), events.toList)
    }

    test("reading non-existing stream raises stream not found") {

      val id = genStreamId(s"${streamPrefix}non_existing_forwards_")
      val ex = StreamNotFound(id.stringValue)

      assertIO(read(id, Start, Forwards, 1).attempt, Left(ex))
    }

    test("reading deleted stream raises stream not found") {

      val id = genStreamId(s"${streamPrefix}stream_deleted_forwards_")
      val ex = StreamNotFound(id.stringValue)

      streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
        streams.delete(id, StreamState.Any) *>
        assertIO(read(id, Start, Forwards, 5).attempt, Left(ex))
    }

    test("reading tombstoned stream raises stream deleted") {

      val id = genStreamId(s"${streamPrefix}stream_tombstoned_forwards_")
      val ex = StreamDeleted(id.stringValue)

      streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
        streams.tombstone(id, StreamState.Any) *>
        assertIO(read(id, Start, Forwards, 5).attempt, Left(ex))
    }

    test("reading single event from arbitrary stream position") {
      assertIO(readData(id, StreamPosition(7), Forwards, 1).map(_.lastOption), events.get(7))
    }

    test("reading from arbitrary stream position") {
      assertIO(readData(id, StreamPosition(3L), Forwards, 2), events.toList.slice(3, 5))
    }

    test("max count is respected") {
      assertIO(
        streams
          .readStream(id, Start, Forwards, events.length / 2L, resolveLinkTos = false)
          .take(events.length.toLong)
          .compile
          .toList
          .map(_.size),
        events.length / 2
      )
    }

  }

  group("read stream backwards") {

    test("reading one from start yields first event") {
      readData(id, Start, Backwards, 1).map { e =>
        assertEquals(e.lastOption, Some(events.head))
        assertEquals(e.size, 1)
      }
    }

    test("reading one from end yields last event") {
      readData(id, End, Backwards, 1).map { es =>
        assertEquals(es.headOption, Some(events.last))
        assertEquals(es.size, 1)
      }
    }

    test("reading from end with max count <= 0 yields no events") {
      assertIOBoolean(read(id, End, Backwards, -1).map(_.isEmpty)) *>
        assertIOBoolean(read(id, End, Backwards, 0).map(_.isEmpty))
    }

    test("events are in reverse order as written") {
      assertIO(readData(id, End, Backwards).map(_.reverse), events.toList)
    }

    test("reading non-existing stream raises stream not found") {
      val id = genStreamId(s"${streamPrefix}non_existing_backwards_")
      val ex = StreamNotFound(id.stringValue)
      assertIO(read(id, End, Backwards, 1).attempt, Left(ex))
    }

    test("reading deleted stream raises stream not found") {

      val id = genStreamId(s"${streamPrefix}stream_deleted_backwards_")
      val ex = StreamNotFound(id.stringValue)

      streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
        streams.delete(id, StreamState.Any) *>
        assertIO(read(id, End, Backwards, 5).attempt, Left(ex))
    }

    test("reading tombstoned stream raises stream deleted") {

      val id = genStreamId(s"${streamPrefix}stream_tombstoned_backwards_")
      val ex = StreamDeleted(id.stringValue)

      streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
        streams.tombstone(id, StreamState.Any) *>
        assertIO(read(id, End, Backwards, 5).attempt, Left(ex))
    }

    test("reading single event from arbitrary stream position") {
      assertIO(readData(id, StreamPosition(20), Backwards, 1).map(_.lastOption), events.get(20))
    }

    test("reading from arbitrary stream position") {
      assertIO(readData(id, StreamPosition(3L), Backwards, 2), events.toList.slice(2, 4).reverse)
    }

    test("max count is respected") {
      assertIO(
        streams
          .readStream(id, End, Backwards, events.length / 2L, resolveLinkTos = false)
          .take(events.length.toLong)
          .compile
          .toList
          .map(_.size),
        events.length / 2
      )
    }

  }
