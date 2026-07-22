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

import java.util as ju
import cats.syntax.all.*
import cats.effect.{IO, Resource}
import com.google.protobuf.ByteString
import com.google.protobuf.struct.Value
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.{Metadata, Status, StatusRuntimeException}
import io.kurrentdb.protocol.v2.streams.*
import scodec.bits.ByteVector
import sec.api.exceptions.StreamNotFound
import sec.arbitraries.*

/** Fault-harness coverage for v2 multi-stream append against a real node: v1 read roundtrip of
  * v2-written records, user-property metadata synthesis, request atomicity, guard semantics on
  * unwritten streams, and typed error surfacing through the client API. The raw-stub tests
  * intentionally remain: they pin the wire-level behavior the public API is built on.
  */
class MultiStreamAppendSuite extends FSuite:

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
      recordId   = Some(sampleOf[ju.UUID].toString),
      properties = Map.empty,
      schema     = Some(SchemaInfo(format = SchemaFormat.SCHEMA_FORMAT_JSON, name = schemaName)),
      data       = ByteString.copyFromUtf8(payload),
      stream     = stream
    )

  private def noStream(stream: String): ConsistencyCheck =
    ConsistencyCheck().withStreamState(ConsistencyCheck.StreamStateCheck(stream = stream, expectedState = -1L))

  /** Distinguishes "feature not enabled" from any other failure. */
  private def explained[A](fa: IO[A]): IO[A] =
    fa.adaptError {
      case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.UNIMPLEMENTED =>
        new AssertionError(
          "v2 StreamsService.AppendRecords is not enabled on this server",
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
        _    <- IO.println(s"[v2] v1 view of v2 record: contentType=${ed.contentType}, metadata=${ed.metadata}")
      yield ()
    }
  }

  test("v2 user properties materialize in the v1 metadata view") {
    (v2Stub, mkClient()).tupled.use { case (stub, client) =>
      given Metadata = new Metadata()

      val sid  = genStreamId("fit_v2_props_")
      val name = sid.stringValue
      // v2 has no raw metadata bytes: the server synthesizes v1 metadata as JSON with
      // $schema.* keys. This decides whether user properties merge into that same object.
      val rec = record(name).copy(properties =
        Map(
          "tenant"  -> Value(Value.Kind.StringValue("acme")),
          "attempt" -> Value(Value.Kind.NumberValue(2)),
          "replay"  -> Value(Value.Kind.BoolValue(true))
        )
      )

      for
        _    <- explained(stub.appendRecords(AppendRecordsRequest(Seq(rec), Seq(noStream(name)))))
        evs  <- client.streams
                  .readStream(sid, StreamPosition.Start, Direction.Forwards, 10L, resolveLinkTos = false)
                  .compile
                  .toList
        json  = new String(evs.head.record.eventData.metadata.toArray, "UTF-8")
        _    <- IO.println(s"[v2] metadata with user properties: $json")
        _    <- IO(assert(json.contains("tenant"), s"user properties should surface in v1 metadata; got: $json"))
      yield ()
    }
  }

  test("typed appends and guards map end-to-end and results decode") {
    (v2Stub, mkClient()).tupled.use { case (stub, client) =>
      given Metadata = new Metadata()
      import cats.data.NonEmptyList as CNel

      val aId = genStreamId("fit_v2_map_a_")
      val bId = genStreamId("fit_v2_map_b_")
      val gId = genStreamId("fit_v2_map_g_")
      val cId = genStreamId("fit_v2_map_c_")

      def rec = RecordData(
        sampleOf[ju.UUID],
        Schema.json(schemaName),
        scodec.bits.ByteVector.view(payload.getBytes("UTF-8")),
        Properties.of("tenant" -> PropertyValue.Str("acme")).fold(m => fail(m), identity)
      )

      val appends = CNel.of(
        StreamAppend(aId, StreamState.NoStream, CNel.one(rec)),
        StreamAppend(bId, StreamState.NoStream, CNel.of(rec, rec))
      )

      // Guard on an unwritten stream, trivially satisfied (gId does not exist yet).
      val ok = mapping.streamsV2.mkAppendRecordsRequest(appends, List(StreamGuard(gId, StreamState.NoStream)))

      // Violated guard: require gId to exist - it still does not - and assert the violation
      // names the guarded stream, proving guards work server-side for unwritten streams.
      val badAppends = CNel.one(StreamAppend(cId, StreamState.NoStream, CNel.one(rec)))
      val badGuard   = StreamGuard(gId, StreamState.StreamExists)
      val bad        = mapping.streamsV2.mkAppendRecordsRequest(badAppends, List(badGuard))

      for
        resp   <- explained(stub.appendRecords(ok))
        result <- mapping.streamsV2.mkMultiAppendResult[IO](appends)(resp)
        _      <- IO(assertEquals(
                    result.streamPositions.toList.toMap,
                    Map(aId -> StreamPosition(0L), bId -> StreamPosition(1L))
                  ))
        res    <- explained(stub.appendRecords(bad)).attempt
        _      <- IO(res match
                    case Left(e: StatusRuntimeException) =>
                      grpc.convertV2.convertToEs(e) match
                        case Some(v: exceptions.AppendConsistencyViolation) =>
                          assertEquals(v.violations.map(_.streamId), List(gId.stringValue))
                        case other => fail(s"expected AppendConsistencyViolation naming the guard, got $other")
                    case other => fail(s"expected guard violation, got $other"))
      yield ()
    }
  }

  test("multiStreamAppend raises typed exceptions through the client API") {
    mkClient().use { client =>
      import cats.data.NonEmptyList as CNel

      val aId = genStreamId("fit_v2_api_a_")
      val bId = genStreamId("fit_v2_api_b_")

      def rec = RecordData(
        sampleOf[ju.UUID],
        Schema.json(schemaName),
        scodec.bits.ByteVector.view(payload.getBytes("UTF-8")),
        Properties.empty
      )

      val appends = CNel.of(
        StreamAppend(aId, StreamState.NoStream, CNel.one(rec)),
        StreamAppend(bId, StreamState.NoStream, CNel.of(rec, rec))
      )

      for
        r   <- client.streams.multiStreamAppend(appends)
        _   <- IO(assertEquals(
                 r.streamPositions.toList.toMap,
                 Map(aId -> StreamPosition(0L), bId -> StreamPosition(1L))
               ))
        // The error adapter must surface a violated guard as the typed exception directly.
        bad  = CNel.one(StreamAppend(genStreamId("fit_v2_api_c_"), StreamState.NoStream, CNel.one(rec)))
        res <- client.streams.multiStreamAppend(bad, List(StreamGuard(aId, StreamState.NoStream))).attempt
        _   <- IO(res match
                 case Left(v: exceptions.AppendConsistencyViolation) =>
                   assertEquals(v.violations.map(_.streamId), List(aId.stringValue))
                 case other => fail(s"expected typed AppendConsistencyViolation, got $other"))
      yield ()
    }
  }

  test("all violated checks are reported, not only the first") {
    mkClient().use { client =>
      import cats.data.NonEmptyList as CNel

      val aId = genStreamId("fit_v2_viol_a_")
      val bId = genStreamId("fit_v2_viol_b_")

      def rec = RecordData(
        sampleOf[ju.UUID],
        Schema.json(schemaName),
        scodec.bits.ByteVector.view(payload.getBytes("UTF-8")),
        Properties.empty
      )

      // Both streams expect existence, neither exists: the details must carry both violations,
      // per the errors.proto contract that all violated checks are included.
      val appends = CNel.of(
        StreamAppend(aId, StreamState.StreamExists, CNel.one(rec)),
        StreamAppend(bId, StreamState.StreamExists, CNel.one(rec))
      )

      client.streams.multiStreamAppend(appends).attempt.map {
        case Left(v: exceptions.AppendConsistencyViolation) =>
          assertEquals(
            v.violations.map(x => (x.streamId, x.expected, x.actual)).toSet,
            Set(
              (aId.stringValue, ExpectedCondition.StreamExists, ActualCondition.NotFound),
              (bId.stringValue, ExpectedCondition.StreamExists, ActualCondition.NotFound)
            )
          )
        case other => fail(s"expected AppendConsistencyViolation with both streams, got $other")
      }
    }
  }

  test("exact stream position expectations are violated precisely and honored when correct") {
    mkClient().use { client =>
      import cats.data.NonEmptyList as CNel

      val cId = genStreamId("fit_v2_exact_")

      def rec = RecordData(
        sampleOf[ju.UUID],
        Schema.json(schemaName),
        scodec.bits.ByteVector.view(payload.getBytes("UTF-8")),
        Properties.empty
      )

      def append(expected: StreamState) =
        client.streams.multiStreamAppend(CNel.one(StreamAppend(cId, expected, CNel.one(rec))))

      for
        _   <- append(StreamState.NoStream) // stream position 0
        res <- append(StreamPosition(5L)).attempt
        _   <- IO(res match
                 case Left(v: exceptions.AppendConsistencyViolation) =>
                   assertEquals(
                     v.violations.map(x => (x.streamId, x.expected, x.actual)),
                     List(
                       (cId.stringValue, ExpectedCondition.AtPosition(StreamPosition(5L)), ActualCondition.AtPosition(StreamPosition(0L)))
                     )
                   )
                 case other => fail(s"expected violation with exact expected/actual, got $other"))
        ok  <- append(StreamPosition(0L))
        _   <- IO(assertEquals(ok.streamPositions.toList, List(cId -> StreamPosition(1L))))
      yield ()
    }
  }

  test("appends to tombstoned streams are promoted to StreamTombstoned") {
    mkClient().use { client =>
      import cats.data.NonEmptyList as CNel

      val dId = genStreamId("fit_v2_tomb_")

      def rec = RecordData(
        sampleOf[ju.UUID],
        Schema.json(schemaName),
        scodec.bits.ByteVector.view(payload.getBytes("UTF-8")),
        Properties.empty
      )

      // The server refuses an append to a tombstoned stream as a consistency violation whose actual
      // condition is Tombstoned; multiStreamAppend0 promotes that to StreamTombstoned for an append
      // target (a guard would keep the AppendConsistencyViolation).
      for
        _   <- client.streams.appendToStream(dId, StreamState.NoStream, genEvents(1))
        _   <- client.streams.tombstone(dId, StreamState.StreamExists)
        res <- client.streams
                 .multiStreamAppend(CNel.one(StreamAppend(dId, StreamState.NoStream, CNel.one(rec))))
                 .attempt
        _   <- IO(res match
                 case Left(e: exceptions.StreamTombstoned) => assertEquals(e.streamId, dId.stringValue)
                 case other => fail(s"expected StreamTombstoned, got $other"))
      yield ()
    }
  }

  test("write expectations across stream states follow the server matrix") {
    mkClient().use { client =>
      import cats.data.NonEmptyList as CNel
      import StreamState.{Any, NoStream, StreamExists}

      def rec = RecordData(
        sampleOf[ju.UUID],
        Schema.json(schemaName),
        scodec.bits.ByteVector.view(payload.getBytes("UTF-8")),
        Properties.empty
      )

      def notFound: IO[StreamId.Id]   = IO(genStreamId("fit_v2_wm_"))
      def atZero: IO[StreamId.Id]     =
        notFound.flatTap(id => client.streams.appendToStream(id, NoStream, genEvents(1)))
      def deleted: IO[StreamId.Id]    =
        atZero.flatTap(id => client.streams.delete(id, StreamExists))
      def tombstoned: IO[StreamId.Id] =
        atZero.flatTap(id => client.streams.tombstone(id, StreamExists))

      def write(id: StreamId.Id, expected: StreamState): IO[Boolean] =
        client.streams
          .multiStreamAppend(CNel.one(StreamAppend(id, expected, CNel.one(rec))))
          .attempt
          .map(_.isRight)

      // Expected outcomes per the matrix pinned by the official clients. Exception types are
      // covered by the targeted tests; here the semantics under test is accept/reject per cell.
      val cases: List[(String, StreamState, IO[StreamId.Id], Boolean)] = List(
        ("NoStream on not-found", NoStream, notFound, true),
        ("NoStream on existing", NoStream, atZero, false),
        ("NoStream on deleted", NoStream, deleted, true),
        ("NoStream on tombstoned", NoStream, tombstoned, false),
        ("Any on not-found", Any, notFound, true),
        ("Any on existing", Any, atZero, true),
        ("Any on deleted", Any, deleted, true),
        ("Any on tombstoned", Any, tombstoned, false),
        ("StreamExists on not-found", StreamExists, notFound, false),
        ("StreamExists on existing", StreamExists, atZero, true),
        ("StreamExists on deleted", StreamExists, deleted, false),
        ("StreamExists on tombstoned", StreamExists, tombstoned, false),
        ("exact on not-found", StreamPosition(0L), notFound, false),
        ("exact on existing", StreamPosition(0L), atZero, true),
        // A soft-deleted stream keeps its last surviving position, so an exact expectation matching
        // it is honoured and the append succeeds - the write and guard matrices agree here. True
        // only because atZero leaves the survivor at revision 0 and we expect exactly 0.
        ("exact on deleted", StreamPosition(0L), deleted, true),
        ("exact on tombstoned", StreamPosition(0L), tombstoned, false)
      )

      cases.traverse_ { case (desc, expected, mkStream, succeeds) =>
        mkStream.flatMap(write(_, expected)).map(r => assertEquals(r, succeeds, desc))
      }
    }
  }

  test("guard expectations across stream states follow the server matrix") {
    mkClient().use { client =>
      import cats.data.NonEmptyList as CNel
      import StreamState.{NoStream, StreamExists}

      def rec = RecordData(
        sampleOf[ju.UUID],
        Schema.json(schemaName),
        scodec.bits.ByteVector.view(payload.getBytes("UTF-8")),
        Properties.empty
      )

      def notFound: IO[StreamId.Id]   = IO(genStreamId("fit_v2_gm_"))
      def atZero: IO[StreamId.Id]     =
        notFound.flatTap(id => client.streams.appendToStream(id, NoStream, genEvents(1)))
      def deleted: IO[StreamId.Id]    =
        atZero.flatTap(id => client.streams.delete(id, StreamExists))
      def tombstoned: IO[StreamId.Id] =
        atZero.flatTap(id => client.streams.tombstone(id, StreamExists))

      def guarded(guard: StreamGuard): IO[Boolean] =
        client.streams
          .multiStreamAppend(
            CNel.one(StreamAppend(genStreamId("fit_v2_gmw_"), NoStream, CNel.one(rec))),
            List(guard)
          )
          .attempt
          .map(_.isRight)

      // Note the asymmetry with the write matrix: a NoStream guard passes on a tombstoned
      // stream - checks treat tombstoned as nonexistent - while writes reject it outright.
      val cases: List[(String, StreamState, IO[StreamId.Id], Boolean)] = List(
        ("NoStream guard on not-found", NoStream, notFound, true),
        ("NoStream guard on existing", NoStream, atZero, false),
        ("NoStream guard on deleted", NoStream, deleted, true),
        ("NoStream guard on tombstoned", NoStream, tombstoned, true),
        ("StreamExists guard on not-found", StreamExists, notFound, false),
        ("StreamExists guard on existing", StreamExists, atZero, true),
        ("StreamExists guard on deleted", StreamExists, deleted, false),
        ("StreamExists guard on tombstoned", StreamExists, tombstoned, false),
        ("exact guard on not-found", StreamPosition(0L), notFound, false),
        ("exact guard on existing", StreamPosition(0L), atZero, true),
        // A check on a soft-deleted stream is evaluated against its last surviving position, so an
        // exact check matching it passes; NoStream also passes (a check treats deleted as absent)
        // and StreamExists fails. Confirmed against the server's AppendRecords CheckOnly suite
        // (WhenExpectingRevision/NoStream/Exists). True here only because atZero leaves the
        // survivor at revision 0 and we check exactly 0.
        ("exact guard on deleted", StreamPosition(0L), deleted, true),
        ("exact guard on tombstoned", StreamPosition(0L), tombstoned, false)
      )

      cases.traverse_ { case (desc, expected, mkStream, succeeds) =>
        mkStream.flatMap(id => guarded(StreamGuard(id, expected))).map(r => assertEquals(r, succeeds, desc))
      }
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
        _    <- IO(res match
                  case Left(e: StatusRuntimeException) =>
                    // The structured details from a real server must convert.
                    grpc.convertV2.convertToEs(e) match
                      case Some(v: exceptions.AppendConsistencyViolation) =>
                        assertEquals(v.violations.map(_.streamId), List(a))
                        assertEquals(v.violations.map(_.actual), List(ActualCondition.AtPosition(StreamPosition(0L))))
                      case other => fail(s"expected AppendConsistencyViolation from convertV2, got $other")
                  case other => fail(s"expected StatusRuntimeException, got $other"))
        _    <- res.swap.toOption.traverse_ {
                  case e: StatusRuntimeException =>
                    // Text-only descriptions would force parsing messages; structured
                    // details (google.rpc.Status in the standard trailer) enable typed exceptions.
                    val key     = Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER)
                    val details = Option(e.getTrailers)
                      .flatMap(t => Option(t.get(key)))
                      .map(bytes => com.google.rpc.Status.parseFrom(bytes).details.map(_.typeUrl).mkString(", "))
                    IO.println(s"[v2] conflict: ${e.getStatus.getCode}: ${e.getStatus.getDescription}") *>
                      IO.println(s"[v2] structured details: ${details.getOrElse("<none in trailers>")}")
                  case t =>
                    IO.println(s"[v2] conflict error shape: ${t.getClass.getName}: ${t.getMessage}")
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
