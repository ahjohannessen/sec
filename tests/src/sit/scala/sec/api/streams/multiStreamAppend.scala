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

import java.util.UUID
import cats.data.NonEmptyList
import cats.effect.IO
import scodec.bits.ByteVector

/** Complements the fit coverage, which runs insecure: this exercises multiStreamAppend on the
  * TLS + credentials path, so the v2 stub's Context handling (auth, authority) is verified too.
  */
class MultiStreamAppendSuite extends SnSuite:

  test("multiStreamAppend over TLS roundtrips and raises typed exceptions") {

    val aId = genStreamId("sit_msa_a_")
    val bId = genStreamId("sit_msa_b_")

    def rec = RecordData(
      UUID.randomUUID(),
      Schema.json("sit-msa-event"),
      ByteVector.view("{}".getBytes("UTF-8")),
      Properties.empty
    )

    val appends = NonEmptyList.of(
      StreamAppend(aId, StreamState.NoStream, NonEmptyList.one(rec)),
      StreamAppend(bId, StreamState.NoStream, NonEmptyList.of(rec, rec))
    )

    for
      r    <- client.streams.multiStreamAppend(appends)
      _    <- IO(assert(
                r.revisions.toList.toMap == Map(aId -> StreamPosition(0L), bId -> StreamPosition(1L)),
                s"unexpected revisions: ${r.revisions}"
              ))
      read <- client.streams
                .readStream(aId, StreamPosition.Start, Direction.Forwards, 10L, resolveLinkTos = false)
                .compile
                .toList
      _    <- IO(assertEquals(read.map(_.record.eventData.eventType.stringValue), List("sit-msa-event")))
      // Single-record transaction: the event's log position must equal the reported transaction
      // position, verifying the commit == prepare mapping of the v2 single-position response.
      cId  = genStreamId("sit_msa_c_")
      rc  <- client.streams.multiStreamAppend(NonEmptyList.one(
               StreamAppend(cId, StreamState.NoStream, NonEmptyList.one(rec))
             ))
      cEv <- client.streams
               .readStream(cId, StreamPosition.Start, Direction.Forwards, 1L, resolveLinkTos = false)
               .compile
               .lastOrError
      _   <- IO(assertEquals(cEv.record.logPosition, rc.position))
      bad   = NonEmptyList.one(StreamAppend(aId, StreamState.NoStream, NonEmptyList.one(rec)))
      res  <- client.streams.multiStreamAppend(bad).attempt
      _    <- IO(res match
                case Left(v: exceptions.AppendConsistencyViolation) =>
                  assertEquals(v.violations.map(_.streamId), List(aId.stringValue))
                case other => fail(s"expected AppendConsistencyViolation, got $other"))
    yield ()
  }
