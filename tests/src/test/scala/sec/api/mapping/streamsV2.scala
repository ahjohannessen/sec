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

import java.util.UUID
import cats.data.NonEmptyList
import io.kurrentdb.protocol.v2.streams as pv2
import scodec.bits.ByteVector
import sec.api.v2.*
import sec.arbitraries.*

class StreamsV2MappingSuite extends SecSuite:

  private def sid(prefix: String): StreamId.Id = sampleOfGen(idGen.genStreamIdNormal(prefix))

  private def rec: Record =
    Record(UUID.randomUUID(), Schema.json("t"), ByteVector.view("{}".getBytes("UTF-8")), Properties.empty)

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

  test("request couples one check to every written stream and appends guards after") {
    val (a, b, g) = (sid("v2m_a_"), sid("v2m_b_"), sid("v2m_g_"))
    val appends   = NonEmptyList.of(
      StreamAppend(a, StreamState.NoStream, NonEmptyList.of(rec, rec)),
      StreamAppend(b, StreamPosition(3L), NonEmptyList.one(rec))
    )
    val req = streamsV2.mkAppendRecordsRequest(appends, List(StreamGuard(g, StreamState.StreamExists)))

    assertEquals(req.records.map(_.stream).toList, List(a, a, b).map(_.stringValue))
    assertEquals(
      req.checks.toList.flatMap(_.`type`.streamState).map(c => c.stream -> c.expectedState),
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
      Right(MultiAppendResult(42L, NonEmptyList.one(a -> StreamPosition(0L))))
    )

    val unknown = pv2.AppendRecordsResponse(position = 42L, revisions = Seq(pv2.StreamRevision("other", 0L)))
    assert(streamsV2.mkMultiAppendResult[A](appends)(unknown).isLeft)

    val empty = pv2.AppendRecordsResponse(position = 42L, revisions = Seq.empty)
    assert(streamsV2.mkMultiAppendResult[A](appends)(empty).isLeft)
  }
