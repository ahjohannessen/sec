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

import scala.concurrent.duration._
import cats.effect.IO
import cats.syntax.all._
import munit.Location
import sec.api.MetaStreams.Result
import sec.api.exceptions.WrongExpectedState
import sec.helpers.implicits._
import sec.syntax.all._

class MetaStreamsSuite extends SnSpec {

  import StreamState.NoStream
  import StreamPosition.{exact, Start}

  val mkStreamId: String => StreamId.Id =
    usecase => genStreamId(s"meta_streams_${genIdentifier}_$usecase")

  ///

  test("work with existing stream and no metadata exists") {

    val sid = mkStreamId("works_with_existing_and_no_metadata")

    streams.appendToStream(sid, NoStream, genEvents(1)) >>
      metaStreams.getMetadata(sid).map(m => assertEquals(m, None))
  }

  test("work with empty metadata") {

    val sid = mkStreamId("works_with_empty_metadata")

    for {
      wr <- metaStreams.setMetadata(sid, NoStream, StreamMetadata.empty)
      mr <- metaStreams.getMetadata(sid)
    } yield {
      assertEquals(wr.streamPosition, Start)
      assertEquals(mr, Some(Result(wr.streamPosition, StreamMetadata.empty)))
    }
  }

  test("getting metadata returns latest stream position") {

    val meta1 = StreamMetadata.empty
      .withMaxCount(MaxCount(17).unsafe)
      .withMaxAge(MaxAge(1.day).unsafe)
      .withTruncateBefore(exact(10))
      .withCacheControl(CacheControl(3.days).unsafe)

    val meta2 = StreamMetadata.empty
      .withMaxCount(MaxCount(37).unsafe)
      .withMaxAge(MaxAge(12.hours).unsafe)
      .withTruncateBefore(exact(24))
      .withCacheControl(CacheControl(36.hours).unsafe)

    // using expected stream state no stream for first write and exact for second write

    val sid1 = mkStreamId("get_metadata_returns_latest_exact")
    val test1: IO[Unit] = for {
      wr1 <- metaStreams.setMetadata(sid1, NoStream, meta1)
      mr1 <- metaStreams.getMetadata(sid1)
      wr2 <- metaStreams.setMetadata(sid1, wr1.streamPosition, meta2)
      mr2 <- metaStreams.getMetadata(sid1)
    } yield {
      assertEquals(wr1.streamPosition, Start)
      assertEquals(wr2.streamPosition, exact(1L))
      assertEquals(mr1, Some(Result(wr1.streamPosition, meta1)))
      assertEquals(mr2, Some(Result(wr2.streamPosition, meta2)))
    }

    // using expected stream state any for first and any for second write

    val sid2 = mkStreamId("get_metadata_returns_latest_any")
    val test2 = for {
      _   <- metaStreams.setMetadata(sid2, StreamState.Any, meta1)
      mr1 <- metaStreams.getMetadata(sid2)
      _   <- metaStreams.setMetadata(sid2, StreamState.Any, meta2)
      mr2 <- metaStreams.getMetadata(sid2)
    } yield {
      assertEquals(mr1, Some(Result(Start, meta1)))
      assertEquals(mr2, Some(Result(exact(1L), meta2)))
    }

    test1 >> test2
  }

  test("setting metadata with wrong expected stream state raises") {

    val sid = mkStreamId("set_metadata_with_wrong_expected_revision_raises")

    metaStreams.setMetadata(sid, exact(2), StreamMetadata.empty).attempt.map { r =>
      assertEquals(r, Left(WrongExpectedState(sid.metaId, exact(2), StreamState.NoStream)))
    }
  }

  ///

  def testFinedGrainedApiFor(name: String)(body: => Any)(implicit loc: Location) =
    test(s"fine-grained api for $name")(body)

  testFinedGrainedApiFor("max age") {
    for {
      sid <- mkStreamId("max_age_api").pure[IO]
      ma1 <- metaStreams.getMaxAge(sid)
      wr1 <- metaStreams.setMaxAge(sid, NoStream, 1.day)
      ma2 <- metaStreams.getMaxAge(sid)
      wr2 <- metaStreams.unsetMaxAge(sid, wr1.streamPosition)
      ma3 <- metaStreams.getMaxAge(sid)
    } yield {
      assertEquals(ma1, None)
      assertEquals(wr1.streamPosition, Start)
      assertEquals(ma2, Some(Result[Option[MaxAge]](Start, Some(MaxAge(1.day).unsafe))))
      assertEquals(wr2.streamPosition, exact(1L))
      assertEquals(ma3, Some(Result[Option[MaxAge]](wr2.streamPosition, None)))
    }
  }

  testFinedGrainedApiFor("max count") {
    for {
      sid <- mkStreamId("max_count_api").pure[IO]
      ma1 <- metaStreams.getMaxCount(sid)
      wr1 <- metaStreams.setMaxCount(sid, NoStream, 10)
      ma2 <- metaStreams.getMaxCount(sid)
      wr2 <- metaStreams.unsetMaxCount(sid, wr1.streamPosition)
      ma3 <- metaStreams.getMaxCount(sid)
    } yield {
      assertEquals(ma1, None)
      assertEquals(wr1.streamPosition, Start)
      assertEquals(ma2, Some(Result[Option[MaxCount]](Start, Some(MaxCount(10).unsafe))))
      assertEquals(wr2.streamPosition, exact(1L))
      assertEquals(ma3, Some(Result[Option[MaxCount]](wr2.streamPosition, None)))
    }
  }

  testFinedGrainedApiFor("cache control") {
    for {
      sid <- mkStreamId("cache_control_api").pure[IO]
      ma1 <- metaStreams.getCacheControl(sid)
      wr1 <- metaStreams.setCacheControl(sid, NoStream, 20.days)
      ma2 <- metaStreams.getCacheControl(sid)
      wr2 <- metaStreams.unsetCacheControl(sid, wr1.streamPosition)
      ma3 <- metaStreams.getCacheControl(sid)
    } yield {
      assertEquals(ma1, None)
      assertEquals(wr1.streamPosition, Start)
      assertEquals(ma2, Some(Result[Option[CacheControl]](Start, Some(CacheControl(20.days).unsafe))))
      assertEquals(wr2.streamPosition, exact(1L))
      assertEquals(ma3, Some(Result[Option[CacheControl]](wr2.streamPosition, None)))
    }
  }

  testFinedGrainedApiFor("acl") {
    for {
      sid <- mkStreamId("acl_api").pure[IO]
      rr  <- StreamAcl.empty.copy(readRoles = Set("role-1")).pure[IO]
      ma1 <- metaStreams.getAcl(sid)
      wr1 <- metaStreams.setAcl(sid, NoStream, rr)
      ma2 <- metaStreams.getAcl(sid)
      wr2 <- metaStreams.unsetAcl(sid, wr1.streamPosition)
      ma3 <- metaStreams.getAcl(sid)
    } yield {
      assertEquals(ma1, None)
      assertEquals(wr1.streamPosition, Start)
      assertEquals(ma2, Some(Result[Option[StreamAcl]](Start, Some(rr))))
      assertEquals(wr2.streamPosition, exact(1L))
      assertEquals(ma3, Some(Result[Option[StreamAcl]](wr2.streamPosition, None)))
    }
  }

  testFinedGrainedApiFor("truncate before") {
    for {
      sid <- mkStreamId("truncate_before_api").pure[IO]
      ma1 <- metaStreams.getTruncateBefore(sid)
      wr1 <- metaStreams.setTruncateBefore(sid, NoStream, 100L)
      ma2 <- metaStreams.getTruncateBefore(sid)
      wr2 <- metaStreams.unsetTruncateBefore(sid, wr1.streamPosition)
      ma3 <- metaStreams.getTruncateBefore(sid)
    } yield {
      assertEquals(ma1, None)
      assertEquals(wr1.streamPosition, Start)
      assertEquals(ma2, Some(Result[Option[StreamPosition.Exact]](Start, Some(exact(100L)))))
      assertEquals(wr2.streamPosition, exact(1L))
      assertEquals(ma3, Some(Result[Option[StreamPosition.Exact]](wr2.streamPosition, None)))
    }
  }

  testFinedGrainedApiFor("custom") {
    for {
      sid <- mkStreamId("custom_api").pure[IO]
      foo <- Foo(bars = (1 to 5).map(i => Bar(i.toString)).toList).pure[IO]
      ma1 <- metaStreams.getCustom[Foo](sid)
      wr1 <- metaStreams.setCustom(sid, NoStream, foo)
      ma2 <- metaStreams.getCustom[Foo](sid)
      wr2 <- metaStreams.unsetCustom(sid, wr1.streamPosition)
      ma3 <- metaStreams.getCustom[Foo](sid)
    } yield {
      assertEquals(ma1, None)
      assertEquals(wr1.streamPosition, Start)
      assertEquals(ma2, Some(Result[Option[Foo]](Start, Some(foo))))
      assertEquals(wr2.streamPosition, exact(1L))
      assertEquals(ma3, Some(Result[Option[Foo]](wr2.streamPosition, None)))
    }
  }

}
