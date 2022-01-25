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

import cats.syntax.all._
import sec.syntax.all._

class ReadsSuite extends SnSuite {

  import StreamState._
  import StreamPosition._

  val streamPrefix = s"reads_read_stream_messages_${genIdentifier}_"

  group("read stream backwards") {

    test("stream not found") {
      val id     = genStreamId(s"${streamPrefix}backwards_not_found_")
      val action = reads.readStreamMessagesBackwards(id, End, 32).compile.lastOrError.map(_.isNotFound)
      assertIOBoolean(action)
    }

    test("stream found") {

      val id     = genStreamId(s"${streamPrefix}backwards_found_")
      val events = genEvents(32)
      val write  = streams.appendToStream(id, NoStream, events)

      write >>
        reads
          .readStreamMessagesBackwards(id, End, 32)
          .compile
          .toList
          .map { ms =>

            val readEvents   = ms.collect { case StreamMessage.Event(e) => e }
            val lastPosition = ms.lastOption.collect { case StreamMessage.LastStreamPosition(l) => l }

            assertEquals(ms.size, 33)
            assertEquals(readEvents.size, 32)
            assertEquals(readEvents.map(_.eventData).reverse, events.toList)
            assertEquals(lastPosition, StreamPosition(31).some)
          }

    }

  }

  group("read stream forwards") {

    test("stream not found") {

      val id     = genStreamId(s"${streamPrefix}forwards_not_found_")
      val action = reads.readStreamMessagesForwards(id, Start, 32).compile.lastOrError.map(_.isNotFound)

      assertIOBoolean(action)
    }

    test("stream found") {

      val id     = genStreamId(s"${streamPrefix}forwards_found_")
      val events = genEvents(32)
      val write  = streams.appendToStream(id, NoStream, events)
      val read   = reads.readStreamMessagesForwards(id, Start, 32).compile.toList
      val result = write >> read

      result.map { ms =>

        val readEvents   = ms.collect { case StreamMessage.Event(e) => e }
        val lastPosition = ms.lastOption.collect { case StreamMessage.LastStreamPosition(l) => l }

        assertEquals(ms.size, 33)
        assertEquals(readEvents.size, 32)
        assertEquals(readEvents.map(_.eventData), events.toList)
        assertEquals(readEvents.headOption.map(_.streamPosition), Start.some)
        assertEquals(readEvents.lastOption.map(_.streamPosition), StreamPosition(31).some)
        assertEquals(lastPosition, StreamPosition(31).some)
      }

    }

    test("stream found truncated") {

      val id       = genStreamId(s"${streamPrefix}forwards_found_truncated")
      val events   = genEvents(64)
      val write    = streams.appendToStream(id, NoStream, events)
      val truncate = client.metaStreams.setTruncateBefore(id, NoStream, 32L)
      val read     = reads.readStreamMessagesForwards(id, Start, 64).compile.toList
      val result   = write >> truncate >> read

      result.map { ms =>

        val firstPosition = ms.headOption.collect { case StreamMessage.FirstStreamPosition(f) => f }
        val readEvents    = ms.collect { case StreamMessage.Event(e) => e }
        val lastPosition  = ms.lastOption.collect { case StreamMessage.LastStreamPosition(l) => l }

        assertEquals(ms.size, 34)
        assertEquals(firstPosition, StreamPosition(32).some)
        assertEquals(readEvents.size, 32)
        assertEquals(readEvents.map(_.eventData), events.toList.drop(32))
        assertEquals(readEvents.headOption.map(_.streamPosition), StreamPosition(32).some)
        assertEquals(readEvents.lastOption.map(_.streamPosition), StreamPosition(63).some)
        assertEquals(lastPosition, StreamPosition(63).some)
      }

    }

  }

}
