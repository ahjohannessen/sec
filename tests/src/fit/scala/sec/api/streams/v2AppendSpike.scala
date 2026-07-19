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

import cats.syntax.all.*
import cats.effect.{IO, Resource}
import com.google.protobuf.ByteString
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.{Metadata, Status, StatusRuntimeException}
import io.kurrentdb.protocol.v2.streams.*
import scodec.bits.ByteVector
import sec.api.exceptions.StreamNotFound

/** Phase 0 spike for v2 multi-stream append. Exit criteria, decided empirically against the stock
  * 26.1 container: (1) StreamsService.AppendRecords is enabled, (2) records written via v2 read
  * back through v1 reads with intact payload and a sane type/contentType mapping, (3) a violated
  * consistency check fails the whole request atomically. Raw generated stubs on purpose - no
  * public API is designed until these answers are in.
  */
class V2AppendSpikeSuite extends FSuite:

  final private val schemaName = "fit-spike-event"
  final private val payload    = """{"n":1}"""

  private def v2Stub: Resource[IO, StreamsServiceFs2Grpc[IO, Metadata]] =
    Resource
      .make(IO.blocking {
        NettyChannelBuilder.forAddress(node.endpoint.address, node.endpoint.port).usePlaintext().build()
      })(ch => IO.blocking { ch.shutdownNow(); () })
      .flatMap(StreamsServiceFs2Grpc.stubResource[IO](_))

  private def record(stream: String): AppendRecord =
    AppendRecord(
      recordId   = Some(java.util.UUID.randomUUID().toString),
      properties = Map.empty,
      schema     = Some(SchemaInfo(format = SchemaFormat.SCHEMA_FORMAT_JSON, name = schemaName)),
      data       = ByteString.copyFromUtf8(payload),
      stream     = stream
    )

  private def noStream(stream: String): ConsistencyCheck =
    ConsistencyCheck().withStreamState(ConsistencyCheck.StreamStateCheck(stream = stream, expectedState = -1L))

  /** Exit criterion 1: distinguish "feature not enabled" from any other failure. */
  private def explained[A](fa: IO[A]): IO[A] =
    fa.adaptError {
      case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.UNIMPLEMENTED =>
        new AssertionError(
          "spike exit criterion NOT met: v2 StreamsService.AppendRecords is not enabled on this server",
          e
        )
    }

  test("v2 append with NoStream check roundtrips through v1 reads") {
    (v2Stub, mkClient()).tupled.use { case (stub, client) =>
      given Metadata = new Metadata()

      val sid  = genStreamId("fit_v2_rt_")
      val name = sid.stringValue
      val req  = AppendRecordsRequest(records = Seq(record(name)), checks = Seq(noStream(name)))

      for
        resp <- explained(stub.appendRecords(req))
        _    <- IO(assertEquals(resp.revisions.toList, List(StreamRevision(name, 0L))))
        _    <- IO(assert(resp.position >= 0L, s"expected a log position, got ${resp.position}"))
        evs  <- client.streams
                  .readStream(sid, StreamPosition.Start, Direction.Forwards, 10L, resolveLinkTos = false)
                  .compile
                  .toList
        _    <- IO(assertEquals(evs.size, 1, s"expected one event via v1 read, got: $evs"))
        ed    = evs.head.record.eventData
        _    <- IO(assertEquals(
                  ed.data,
                  ByteVector.view(payload.getBytes("UTF-8")),
                  "payload bytes must roundtrip v2-append -> v1-read unchanged"
                ))
        _    <- IO(assertEquals(
                  ed.eventType.stringValue,
                  schemaName,
                  s"hypothesis: v1 eventType == v2 schema name; actual mapping: ${ed.eventType.stringValue}"
                ))
        _    <- IO.println(s"[spike] v1 view of v2 record: contentType=${ed.contentType}, metadata=${ed.metadata}")
      yield ()
    }
  }

  test("v2 multi-stream append is atomic; a violated check fails the whole request") {
    (v2Stub, mkClient()).tupled.use { case (stub, client) =>
      given Metadata = new Metadata()

      val a   = genStreamId("fit_v2_ms_a_").stringValue
      val bId = genStreamId("fit_v2_ms_b_")
      val b   = bId.stringValue
      val cId = genStreamId("fit_v2_ms_c_")
      val c   = cId.stringValue

      val ok  = AppendRecordsRequest(Seq(record(a), record(b)), Seq(noStream(a), noStream(b)))
      // a exists after `ok`, so its NoStream check must fail - and c must not be written.
      val bad = AppendRecordsRequest(Seq(record(c), record(a)), Seq(noStream(c), noStream(a)))

      for
        resp <- explained(stub.appendRecords(ok))
        _    <- IO(assertEquals(resp.revisions.map(_.stream).toSet, Set(a, b)))
        _    <- IO(assert(resp.revisions.forall(_.revision == 0L), s"expected revision 0 on both: $resp"))
        res  <- explained(stub.appendRecords(bad)).attempt
        _    <- IO(assert(res.isLeft, s"expected consistency violation, got $res"))
        _    <- res.swap.toOption.traverse_ { t =>
                  IO.println(s"[spike] conflict error shape: ${t.getClass.getName}: ${t.getMessage}")
                }
        cRes <- client.streams
                  .readStream(cId, StreamPosition.Start, Direction.Forwards, 10L, resolveLinkTos = false)
                  .compile
                  .toList
                  .attempt
        _    <- IO(cRes match
                  case Left(_: StreamNotFound) => ()
                  case other                   => fail(s"atomicity violated: stream $c should not exist, got $other"))
      yield ()
    }
  }
