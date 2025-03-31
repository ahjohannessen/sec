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

import scala.concurrent.duration.*
import cats.effect.IO
import cats.syntax.all.*
import sec.api.MetaStreams.Result
import sec.api.exceptions.WrongExpectedState
import sec.helpers.implicits.*
import sec.syntax.all.*

class MetaStreamsSuite extends SnSuite {

  import StreamState.NoStream
  import StreamPosition.Start

  val mkStreamId: String => StreamId.Id =
    usecase => genStreamId(s"meta_streams_${genIdentifier}_$usecase")

  test("existing stream and no metadata exists") {

    val sid = mkStreamId("works_with_existing_and_no_metadata")
    val run = streams.appendToStream(sid, NoStream, genEvents(1)) >> metaStreams.getMetadata(sid)

    assertIO(run, None)
  }

  test("empty metadata") {

    val sid = mkStreamId("works_with_empty_metadata")

    for {
      wr <- metaStreams.setMetadata(sid, NoStream, StreamMetadata.empty)
      mr <- metaStreams.getMetadata(sid)
    } yield {
      assertEquals(wr.streamPosition, Start)
      assertEquals(mr, Some(Result(wr.streamPosition, StreamMetadata.empty)))
    }
  }

  group("getting metadata returns latest stream position") {

    val meta1 = StreamMetadata.empty
      .withMaxCount(MaxCount(17).unsafeGet)
      .withMaxAge(MaxAge(1.day).unsafeGet)
      .withTruncateBefore(StreamPosition(10))
      .withCacheControl(CacheControl(3.days).unsafeGet)

    val meta2 = StreamMetadata.empty
      .withMaxCount(MaxCount(37).unsafeGet)
      .withMaxAge(MaxAge(12.hours).unsafeGet)
      .withTruncateBefore(StreamPosition(24))
      .withCacheControl(CacheControl(36.hours).unsafeGet)

    test("using expected stream state no stream for first write and exact for second write") {

      val sid = mkStreamId("get_metadata_returns_latest_exact")

      for {
        wr1 <- metaStreams.setMetadata(sid, NoStream, meta1)
        mr1 <- metaStreams.getMetadata(sid)
        wr2 <- metaStreams.setMetadata(sid, wr1.streamPosition, meta2)
        mr2 <- metaStreams.getMetadata(sid)
      } yield {
        assertEquals(wr1.streamPosition, Start)
        assertEquals(wr2.streamPosition, StreamPosition(1L))
        assertEquals(mr1, Some(Result(wr1.streamPosition, meta1)))
        assertEquals(mr2, Some(Result(wr2.streamPosition, meta2)))
      }
    }

    test("using expected stream state any for first and any for second write") {

      val sid = mkStreamId("get_metadata_returns_latest_any")

      for {
        _   <- metaStreams.setMetadata(sid, StreamState.Any, meta1)
        mr1 <- metaStreams.getMetadata(sid)
        _   <- metaStreams.setMetadata(sid, StreamState.Any, meta2)
        mr2 <- metaStreams.getMetadata(sid)
      } yield {
        assertEquals(mr1, Some(Result(Start, meta1)))
        assertEquals(mr2, Some(Result(StreamPosition(1L), meta2)))
      }

    }
  }

  test("setting metadata with wrong expected stream state raises") {

    val sid = mkStreamId("set_metadata_with_wrong_expected_revision_raises")
    val run = metaStreams.setMetadata(sid, StreamPosition(2), StreamMetadata.empty).attempt

    assertIO(run, Left(WrongExpectedState(sid.metaId, StreamPosition(2), StreamState.NoStream)))
  }

  group("defines a fine-grained api") {

    test("max age") {

      for {
        sid <- mkStreamId("max_age_api").pure[IO]
        ma1 <- metaStreams.getMaxAge(sid)
        wr1 <- metaStreams.setMaxAge(sid, NoStream, 1.day)
        ma2 <- metaStreams.getMaxAge(sid)
        wr2 <- metaStreams.unsetMaxAge(sid, wr1.streamPosition)
        ma3 <- metaStreams.getMaxAge(sid)
      } yield {
        assert(ma1.isEmpty)
        assertEquals(wr1.streamPosition, Start)
        assertEquals(ma2, Result(Start, MaxAge(1.day).unsafeGet.some).some)
        assertEquals(wr2.streamPosition, StreamPosition(1L))
        assertEquals(ma3, Result(wr2.streamPosition, none[MaxAge]).some)
      }
    }

    test("max count") {

      for {
        sid <- mkStreamId("max_count_api").pure[IO]
        ma1 <- metaStreams.getMaxCount(sid)
        wr1 <- metaStreams.setMaxCount(sid, NoStream, 10)
        ma2 <- metaStreams.getMaxCount(sid)
        wr2 <- metaStreams.unsetMaxCount(sid, wr1.streamPosition)
        ma3 <- metaStreams.getMaxCount(sid)
      } yield {
        assert(ma1.isEmpty)
        assertEquals(wr1.streamPosition, Start)
        assertEquals(ma2, Result(Start, MaxCount(10).unsafeGet.some).some)
        assertEquals(wr2.streamPosition, StreamPosition(1L))
        assertEquals(ma3, Result(wr2.streamPosition, none[MaxCount]).some)
      }
    }

    test("cache control") {

      for {
        sid <- mkStreamId("cache_control_api").pure[IO]
        ma1 <- metaStreams.getCacheControl(sid)
        wr1 <- metaStreams.setCacheControl(sid, NoStream, 20.days)
        ma2 <- metaStreams.getCacheControl(sid)
        wr2 <- metaStreams.unsetCacheControl(sid, wr1.streamPosition)
        ma3 <- metaStreams.getCacheControl(sid)
      } yield {
        assert(ma1.isEmpty)
        assertEquals(wr1.streamPosition, Start)
        assertEquals(ma2, Result(Start, CacheControl(20.days).unsafeGet.some).some)
        assertEquals(wr2.streamPosition, StreamPosition(1L))
        assertEquals(ma3, Result(wr2.streamPosition, none[CacheControl]).some)
      }
    }

    test("acl") {

      for {
        sid <- mkStreamId("acl_api").pure[IO]
        rr  <- StreamAcl.empty.copy(readRoles = Set("role-1")).pure[IO]
        ma1 <- metaStreams.getAcl(sid)
        wr1 <- metaStreams.setAcl(sid, NoStream, rr)
        ma2 <- metaStreams.getAcl(sid)
        wr2 <- metaStreams.unsetAcl(sid, wr1.streamPosition)
        ma3 <- metaStreams.getAcl(sid)
      } yield {
        assert(ma1.isEmpty)
        assertEquals(wr1.streamPosition, Start)
        assertEquals(ma2, Result(Start, rr.some).some)
        assertEquals(wr2.streamPosition, StreamPosition(1L))
        assertEquals(ma3, Result(wr2.streamPosition, none[StreamAcl]).some)
      }
    }

    test("truncate before") {

      for {
        sid <- mkStreamId("truncate_before_api").pure[IO]
        ma1 <- metaStreams.getTruncateBefore(sid)
        wr1 <- metaStreams.setTruncateBefore(sid, NoStream, 100L)
        ma2 <- metaStreams.getTruncateBefore(sid)
        wr2 <- metaStreams.unsetTruncateBefore(sid, wr1.streamPosition)
        ma3 <- metaStreams.getTruncateBefore(sid)
      } yield {
        assert(ma1.isEmpty)
        assertEquals(wr1.streamPosition, Start)
        assertEquals(ma2, Result(Start, StreamPosition(100L).some).some)
        assertEquals(wr2.streamPosition, StreamPosition(1L))
        assertEquals(ma3, Result(wr2.streamPosition, none[StreamPosition.Exact]).some)
      }
    }

    test("custom") {

      for {
        sid <- mkStreamId("custom_api").pure[IO]
        foo <- Foo(bars = (1 to 5).map(i => Bar(i.toString)).toList).pure[IO]
        ma1 <- metaStreams.getCustom[Foo](sid)
        wr1 <- metaStreams.setCustom(sid, NoStream, foo)
        ma2 <- metaStreams.getCustom[Foo](sid)
        wr2 <- metaStreams.unsetCustom(sid, wr1.streamPosition)
        ma3 <- metaStreams.getCustom[Foo](sid)
      } yield {
        assert(ma1.isEmpty)
        assertEquals(wr1.streamPosition, Start)
        assertEquals(ma2, Result(Start, foo.some).some)
        assertEquals(wr2.streamPosition, StreamPosition(1L))
        assertEquals(ma3, Result(wr2.streamPosition, none[Foo]).some)
      }

    }

  }

}
