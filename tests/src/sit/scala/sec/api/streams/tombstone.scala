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
import sec.syntax.all.*
import sec.api.exceptions.*

class TombstoneSuite extends SnSuite:

  import StreamState.*
  import StreamPosition.*

  val streamPrefix = s"streams_tombstone_${genIdentifier}_"

  group("tombstone a stream that does not exist") {

    def mkId(expectedState: StreamState) =
      val st = mkSnakeCase(expectedState.render)
      genStreamId(s"${streamPrefix}non_existing_stream_with_expected_state_${st}_")

    test("works with no stream expected stream state") {
      val state = NoStream
      val id    = mkId(state)
      assertIO_(streams.tombstone(id, state).void)
    }

    test("works with any expected stream state") {
      val state = Any
      val id    = mkId(state)
      assertIO_(streams.tombstone(id, state).void)
    }

    test("raises with wrong expected stream state") {
      val state       = Start
      val id          = mkId(state)
      val expectedMsg = WrongExpectedState.msg(id, Start, NoStream)
      interceptMessageIO[WrongExpectedState](expectedMsg)(streams.tombstone(id, state))
    }

  }

  test("tombstone a stream returns log position") {

    val id     = genStreamId(s"${streamPrefix}return_log_position_")
    val events = genEvents(1)

    for
      wr  <- streams.appendToStream(id, NoStream, events)
      pos <- streams.readAllBackwards(maxCount = 1).compile.lastOrError.map(_.logPosition)
      tr  <- streams.tombstone(id, wr.streamPosition)
    yield assert(tr.logPosition > pos)

  }

  test("tombstone a tombstoned stream raises") {
    val id = genStreamId(s"${streamPrefix}tombstoned_stream_")
    streams.tombstone(id, NoStream) >>
      interceptIO[StreamDeleted](streams.tombstone(id, NoStream))
  }
