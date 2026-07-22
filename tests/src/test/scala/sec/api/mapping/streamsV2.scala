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
package mapping

import cats.data.NonEmptyList
import io.kurrentdb.protocol.v2.streams as pv2
import sec.arbitraries.*

class StreamsV2MappingSuite extends SecSuite:

  private def sid(prefix: String): StreamId.Id = sampleOfGen(idGen.genStreamIdNormal(prefix))

  private def rec: RecordData = sampleOf[RecordData]

  test("expected state uses StreamState sentinel encoding") {
    assertEquals(streamsV2.mkExpectedState(StreamState.NoStream), -1L)
    assertEquals(streamsV2.mkExpectedState(StreamState.Any), -2L)
    assertEquals(streamsV2.mkExpectedState(StreamState.StreamExists), -4L)
    assertEquals(streamsV2.mkExpectedState(StreamPosition(7L)), 7L)
  }

  test("properties reject empty and reserved keys, accept valid ones") {
    assert(Properties.of("$schema.name" -> PropertyValue.Str("x")).isLeft)
    assert(Properties.of("" -> PropertyValue.Bool(true)).isLeft)
    assert(Properties.of("tenant" -> PropertyValue.Str("acme"), "attempt" -> PropertyValue.Num(2)).isRight)
  }

  test("request couples checks to written streams, appends guards, and omits Any") {
    val (a, b, c, g) = (sid("v2m_a_"), sid("v2m_b_"), sid("v2m_c_"), sid("v2m_g_"))
    val appends      = NonEmptyList.of(
      StreamAppend(a, StreamState.NoStream, NonEmptyList.of(rec, rec)),
      StreamAppend(b, StreamPosition(3L), NonEmptyList.one(rec)),
      StreamAppend(c, StreamState.Any, NonEmptyList.one(rec))
    )
    val guards = List(StreamGuard(g, StreamState.StreamExists), StreamGuard(sid("v2m_v_"), StreamState.Any))
    val req    = streamsV2.mkAppendRecordsRequest(appends, guards)

    assertEquals(req.records.map(_.stream).toList, List(a, a, b, c).map(_.stringValue))
    // Any appears in neither writes nor guards: absence of a check is the v2 encoding of Any.
    assertEquals(
      req.checks.toList.flatMap(_.`type`.streamState).map(x => x.stream -> x.expectedState),
      List(a.stringValue -> -1L, b.stringValue -> 3L, g.stringValue -> -4L)
    )
  }

  test("result decoding recovers typed ids and rejects unknown streams") {
    val a       = sid("v2m_r_")
    val appends = NonEmptyList.one(StreamAppend(a, StreamState.NoStream, NonEmptyList.one(rec)))

    type A[T] = Either[Throwable, T]

    val ok = pv2.AppendRecordsResponse(position = 42L, revisions = Seq(pv2.StreamRevision(a.stringValue, 0L)))
    assertEquals(
      streamsV2.mkMultiAppendResult[A](appends)(ok),
      Right(MultiAppendResult(LogPosition.exact(42L, 42L), NonEmptyList.one(a -> StreamPosition(0L))))
    )

    val unknown = pv2.AppendRecordsResponse(position = 42L, revisions = Seq(pv2.StreamRevision("other", 0L)))
    assert(streamsV2.mkMultiAppendResult[A](appends)(unknown).isLeft)

    val empty = pv2.AppendRecordsResponse(position = 42L, revisions = Seq.empty)
    assert(streamsV2.mkMultiAppendResult[A](appends)(empty).isLeft)

    val b        = sid("v2m_r2_")
    val appends2 = appends :+ StreamAppend(b, StreamState.NoStream, NonEmptyList.one(rec))
    val partial  = pv2.AppendRecordsResponse(position = 42L, revisions = Seq(pv2.StreamRevision(a.stringValue, 0L)))
    assert(streamsV2.mkMultiAppendResult[A](appends2)(partial).isLeft, "partial responses must be rejected")
  }
