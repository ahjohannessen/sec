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

// /*
//  * Copyright 2020 Scala EventStoreDB Client
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  *     http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */

// package sec
// package api

// import scala.concurrent.duration._

// import cats.effect.IO
// import cats.syntax.all._
// import sec.api.MetaStreams.Result
// import sec.api.exceptions.WrongExpectedState
// import sec.helpers.implicits._
// import sec.syntax.all._

// class MetaStreamsSuite extends SnSpec {

//   sequential

//   "MetaStreams" should {

//     import StreamState.NoStream
//     import StreamPosition.{exact, Start}

//     val mkStreamId: String => StreamId.Id =
//       usecase => genStreamId(s"meta_streams_${genIdentifier}_$usecase")

//     ///

//     "work with existing stream and no metadata exists" >> {

//       val sid = mkStreamId("works_with_existing_and_no_metadata")

//       streams.appendToStream(sid, NoStream, genEvents(1)) >>
//         metaStreams.getMetadata(sid).map(_ should beNone)
//     }

//     "work with empty metadata" >> {

//       val sid = mkStreamId("works_with_empty_metadata")

//       for {
//         wr <- metaStreams.setMetadata(sid, NoStream, StreamMetadata.empty)
//         mr <- metaStreams.getMetadata(sid)
//       } yield {
//         wr.streamPosition shouldEqual Start
//         mr should beSome(Result(wr.streamPosition, StreamMetadata.empty))
//       }
//     }

//     "getting metadata returns latest stream position" >> {

//       val meta1 = StreamMetadata.empty
//         .withMaxCount(MaxCount(17).unsafe)
//         .withMaxAge(MaxAge(1.day).unsafe)
//         .withTruncateBefore(exact(10))
//         .withCacheControl(CacheControl(3.days).unsafe)

//       val meta2 = StreamMetadata.empty
//         .withMaxCount(MaxCount(37).unsafe)
//         .withMaxAge(MaxAge(12.hours).unsafe)
//         .withTruncateBefore(exact(24))
//         .withCacheControl(CacheControl(36.hours).unsafe)

//       "using expected stream state no stream for first write and exact for second write" >> {

//         val sid = mkStreamId("get_metadata_returns_latest_exact")

//         for {
//           wr1 <- metaStreams.setMetadata(sid, NoStream, meta1)
//           mr1 <- metaStreams.getMetadata(sid)
//           wr2 <- metaStreams.setMetadata(sid, wr1.streamPosition, meta2)
//           mr2 <- metaStreams.getMetadata(sid)
//         } yield {
//           wr1.streamPosition shouldEqual Start
//           wr2.streamPosition shouldEqual exact(1L)
//           mr1 should beSome(Result(wr1.streamPosition, meta1))
//           mr2 should beSome(Result(wr2.streamPosition, meta2))
//         }
//       }

//       "using expected stream state any for first and any for second write" >> {

//         val sid = mkStreamId("get_metadata_returns_latest_any")

//         for {
//           _   <- metaStreams.setMetadata(sid, StreamState.Any, meta1)
//           mr1 <- metaStreams.getMetadata(sid)
//           _   <- metaStreams.setMetadata(sid, StreamState.Any, meta2)
//           mr2 <- metaStreams.getMetadata(sid)
//         } yield {
//           mr1 should beSome(Result(Start, meta1))
//           mr2 should beSome(Result(exact(1L), meta2))
//         }

//       }
//     }

//     "setting metadata with wrong expected stream state raises" >> {

//       val sid = mkStreamId("set_metadata_with_wrong_expected_revision_raises")

//       metaStreams.setMetadata(sid, exact(2), StreamMetadata.empty).attempt.map {
//         _ should beLeft(WrongExpectedState(sid.metaId, exact(2), StreamState.NoStream))
//       }
//     }

//     "define a fine-grained api" >> {

//       "max age" >> {

//         for {
//           sid <- mkStreamId("max_age_api").pure[IO]
//           ma1 <- metaStreams.getMaxAge(sid)
//           wr1 <- metaStreams.setMaxAge(sid, NoStream, 1.day)
//           ma2 <- metaStreams.getMaxAge(sid)
//           wr2 <- metaStreams.unsetMaxAge(sid, wr1.streamPosition)
//           ma3 <- metaStreams.getMaxAge(sid)
//         } yield {
//           ma1 should beNone
//           wr1.streamPosition shouldEqual Start
//           ma2 should beSome(Result(Start, Some(MaxAge(1.day).unsafe)))
//           wr2.streamPosition shouldEqual exact(1L)
//           ma3 should beSome(Result(wr2.streamPosition, None))
//         }
//       }

//       "max count" >> {

//         for {
//           sid <- mkStreamId("max_count_api").pure[IO]
//           ma1 <- metaStreams.getMaxCount(sid)
//           wr1 <- metaStreams.setMaxCount(sid, NoStream, 10)
//           ma2 <- metaStreams.getMaxCount(sid)
//           wr2 <- metaStreams.unsetMaxCount(sid, wr1.streamPosition)
//           ma3 <- metaStreams.getMaxCount(sid)
//         } yield {
//           ma1 should beNone
//           wr1.streamPosition shouldEqual Start
//           ma2 should beSome(Result(Start, Some(MaxCount(10).unsafe)))
//           wr2.streamPosition shouldEqual exact(1L)
//           ma3 should beSome(Result(wr2.streamPosition, None))
//         }
//       }

//       "cache control" >> {

//         for {
//           sid <- mkStreamId("cache_control_api").pure[IO]
//           ma1 <- metaStreams.getCacheControl(sid)
//           wr1 <- metaStreams.setCacheControl(sid, NoStream, 20.days)
//           ma2 <- metaStreams.getCacheControl(sid)
//           wr2 <- metaStreams.unsetCacheControl(sid, wr1.streamPosition)
//           ma3 <- metaStreams.getCacheControl(sid)
//         } yield {
//           ma1 should beNone
//           wr1.streamPosition shouldEqual Start
//           ma2 should beSome(Result(Start, Some(CacheControl(20.days).unsafe)))
//           wr2.streamPosition shouldEqual exact(1L)
//           ma3 should beSome(Result(wr2.streamPosition, None))
//         }
//       }

//       "acl" >> {

//         for {
//           sid <- mkStreamId("acl_api").pure[IO]
//           rr  <- StreamAcl.empty.copy(readRoles = Set("role-1")).pure[IO]
//           ma1 <- metaStreams.getAcl(sid)
//           wr1 <- metaStreams.setAcl(sid, NoStream, rr)
//           ma2 <- metaStreams.getAcl(sid)
//           wr2 <- metaStreams.unsetAcl(sid, wr1.streamPosition)
//           ma3 <- metaStreams.getAcl(sid)
//         } yield {
//           ma1 should beNone
//           wr1.streamPosition shouldEqual Start
//           ma2 should beSome(Result(Start, Some(rr)))
//           wr2.streamPosition shouldEqual exact(1L)
//           ma3 should beSome(Result(wr2.streamPosition, None))
//         }
//       }

//       "truncate before" >> {

//         for {
//           sid <- mkStreamId("truncate_before_api").pure[IO]
//           ma1 <- metaStreams.getTruncateBefore(sid)
//           wr1 <- metaStreams.setTruncateBefore(sid, NoStream, 100L)
//           ma2 <- metaStreams.getTruncateBefore(sid)
//           wr2 <- metaStreams.unsetTruncateBefore(sid, wr1.streamPosition)
//           ma3 <- metaStreams.getTruncateBefore(sid)
//         } yield {
//           ma1 should beNone
//           wr1.streamPosition shouldEqual Start
//           ma2 should beSome(Result(Start, Some(exact(100L))))
//           wr2.streamPosition shouldEqual exact(1L)
//           ma3 should beSome(Result(wr2.streamPosition, None))
//         }
//       }

//       "custom" >> {

//         for {
//           sid <- mkStreamId("custom_api").pure[IO]
//           foo <- Foo(bars = (1 to 5).map(i => Bar(i.toString)).toList).pure[IO]
//           ma1 <- metaStreams.getCustom[Foo](sid)
//           wr1 <- metaStreams.setCustom(sid, NoStream, foo)
//           ma2 <- metaStreams.getCustom[Foo](sid)
//           wr2 <- metaStreams.unsetCustom(sid, wr1.streamPosition)
//           ma3 <- metaStreams.getCustom[Foo](sid)
//         } yield {
//           ma1 should beNone
//           wr1.streamPosition shouldEqual Start
//           ma2 should beSome(Result(Start, Some(foo)))
//           wr2.streamPosition shouldEqual exact(1L)
//           ma3 should beSome(Result(wr2.streamPosition, None))
//         }

//       }

//     }

//   }
// }
