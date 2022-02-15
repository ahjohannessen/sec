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
package mapping

import java.time.{Instant, ZoneOffset}
import java.util.{UUID => JUUID}
import cats.data.NonEmptyList
import cats.syntax.all._
import org.scalacheck.Prop._
import com.eventstore.dbclient.proto.shared.{AllStreamPosition, Empty, UUID}
import com.eventstore.dbclient.proto.{streams => s}
import scodec.bits.ByteVector
import sec.api.exceptions.{StreamNotFound, WrongExpectedState}
import sec.api.mapping.implicits._
import sec.api.mapping.shared._
import sec.api.mapping.streams.{incoming, outgoing}
import sec.arbitraries._
import sec.helpers.implicits._
import sec.helpers.text.encodeToBV

class StreamsMappingSuite extends SecScalaCheckSuite {

  import StreamsMappingSuite._

  group("outgoing") {

    import outgoing._
    import s.ReadReq.Options.AllOptions.AllOption
    import s.ReadReq.Options.StreamOptions.RevisionOption

    val empty = Empty()

    //

    test("uuidOption") {
      assertEquals(uuidOption, s.ReadReq.Options.UUIDOption().withStructured(empty))
    }

    test("LogPosition") {

      assertEquals(
        mapLogPosition(sec.LogPosition.exact(1L, 2L)),
        AllOption.Position(s.ReadReq.Options.Position(1L, 2L))
      )

      assertEquals(mapLogPosition(sec.LogPosition.End), AllOption.End(empty))

      assertEquals(
        mapLogPositionOpt(sec.LogPosition.exact(0L, 0L).some),
        AllOption.Position(s.ReadReq.Options.Position(0L, 0L))
      )

      assertEquals(mapLogPositionOpt(None), AllOption.Start(empty))
    }

    test("mapStreamPosition") {
      assertEquals(mapStreamPosition(sec.StreamPosition(1L)), RevisionOption.Revision(1L))
      assertEquals(mapStreamPosition(sec.StreamPosition.End), RevisionOption.End(empty))
      assertEquals(mapStreamPositionOpt(sec.StreamPosition(0L).some), RevisionOption.Revision(0L))
      assertEquals(mapStreamPositionOpt(None), RevisionOption.Start(empty))
    }

    test("mapDirection") {
      assertEquals(mapDirection(Direction.Forwards), s.ReadReq.Options.ReadDirection.Forwards)
      assertEquals(mapDirection(Direction.Backwards), s.ReadReq.Options.ReadDirection.Backwards)
    }

    test("mapReadEventFilter") {

      import EventFilter._
      import s.ReadReq.Options.FilterOptions
      import s.ReadReq.Options.FilterOptions.Expression
      import s.ReadReq.Options.FilterOption.{Filter, NoFilter}

      def mkOptions(f: EventFilter, maxWindow: Option[Int] = 10.some, multiplier: Int = 1) =
        SubscriptionFilterOptions(f, maxWindow, multiplier).some

      assertEquals(mapReadEventFilter(None), NoFilter(empty))

      assertEquals(
        mapReadEventFilter(mkOptions(streamIdPrefix("a", "b"))),
        Filter(
          FilterOptions()
            .withStreamIdentifier(Expression().withPrefix(List("a", "b")))
            .withMax(10)
            .withCheckpointIntervalMultiplier(1)
        )
      )

      assertEquals(
        mapReadEventFilter(mkOptions(streamIdPrefix("a"), None, 1)),
        Filter(
          FilterOptions()
            .withStreamIdentifier(Expression().withPrefix(List("a")))
            .withCount(empty)
            .withCheckpointIntervalMultiplier(1)
        )
      )

      assertEquals(
        mapReadEventFilter(mkOptions(eventTypePrefix("a", "b"))),
        Filter(
          FilterOptions()
            .withEventType(Expression().withPrefix(List("a", "b")))
            .withMax(10)
            .withCheckpointIntervalMultiplier(1)
        )
      )

      assertEquals(
        mapReadEventFilter(mkOptions(eventTypePrefix("a"), None, 1)),
        Filter(
          FilterOptions()
            .withEventType(Expression().withPrefix(List("a")))
            .withCount(empty)
            .withCheckpointIntervalMultiplier(1)
        )
      )

      assertEquals(
        mapReadEventFilter(mkOptions(streamIdRegex("^[^$].*"))),
        Filter(
          FilterOptions()
            .withStreamIdentifier(Expression().withRegex("^[^$].*"))
            .withMax(10)
            .withCheckpointIntervalMultiplier(1)
        )
      )

      assertEquals(
        mapReadEventFilter(mkOptions(streamIdRegex("^(ns_).+"), None, 1)),
        Filter(
          FilterOptions()
            .withStreamIdentifier(Expression().withRegex("^(ns_).+"))
            .withCount(empty)
            .withCheckpointIntervalMultiplier(1)
        )
      )

      assertEquals(
        mapReadEventFilter(mkOptions(eventTypeRegex("^[^$].*"))),
        Filter(
          FilterOptions()
            .withEventType(Expression().withRegex("^[^$].*"))
            .withMax(10)
            .withCheckpointIntervalMultiplier(1)
        )
      )

      assertEquals(
        mapReadEventFilter(mkOptions(eventTypeRegex("^(ns_).+"), None, 1)),
        Filter(
          FilterOptions()
            .withEventType(Expression().withRegex("^(ns_).+"))
            .withCount(empty)
            .withCheckpointIntervalMultiplier(1)
        )
      )

    }

    test("mkSubscribeToStreamReq") {
      import StreamPosition._

      val sid = StreamId("abc").unsafe

      def test(exclusiveFrom: Option[StreamPosition], resolveLinkTos: Boolean) =
        assertEquals(
          mkSubscribeToStreamReq(sid, exclusiveFrom, resolveLinkTos),
          s
            .ReadReq()
            .withOptions(
              s.ReadReq
                .Options()
                .withStream(s.ReadReq.Options.StreamOptions(sid.esSid.some, mapStreamPositionOpt(exclusiveFrom)))
                .withSubscription(s.ReadReq.Options.SubscriptionOptions())
                .withReadDirection(s.ReadReq.Options.ReadDirection.Forwards)
                .withResolveLinks(resolveLinkTos)
                .withNoFilter(empty)
                .withUuidOption(uuidOption)
            )
        )

      for {
        ef <- List(Option.empty[StreamPosition], Start.some, StreamPosition(1337L).some, End.some)
        rt <- List(true, false)
      } yield test(ef, rt)
    }

    test("mkSubscribeToAllReq") {

      import LogPosition._
      import EventFilter._

      def test(exclusiveFrom: Option[LogPosition], resolveLinkTos: Boolean, filter: Option[SubscriptionFilterOptions]) =
        assertEquals(
          mkSubscribeToAllReq(exclusiveFrom, resolveLinkTos, filter),
          s
            .ReadReq()
            .withOptions(
              s.ReadReq
                .Options()
                .withAll(s.ReadReq.Options.AllOptions(mapLogPositionOpt(exclusiveFrom)))
                .withSubscription(s.ReadReq.Options.SubscriptionOptions())
                .withReadDirection(s.ReadReq.Options.ReadDirection.Forwards)
                .withResolveLinks(resolveLinkTos)
                .withFilterOption(mapReadEventFilter(filter))
                .withUuidOption(uuidOption)
            )
        )

      for {
        ef <- List(Option.empty[LogPosition], Start.some, exact(1337L, 1337L).some, End.some)
        rt <- List(true, false)
        fi <- List(
                Option.empty[SubscriptionFilterOptions],
                SubscriptionFilterOptions(streamIdPrefix("abc"), 64.some, 1).some,
                SubscriptionFilterOptions(eventTypeRegex("^[^$].*")).some
              )
      } yield test(ef, rt, fi)
    }

    test("mkReadStreamReq") {

      val sid = sec.StreamId("abc").unsafe

      def test(rd: Direction, from: StreamPosition, count: Long, rlt: Boolean) =
        assertEquals(
          mkReadStreamReq(sid, from, rd, count, rlt),
          s.ReadReq()
            .withOptions(
              s.ReadReq
                .Options()
                .withStream(s.ReadReq.Options.StreamOptions(sid.esSid.some, mapStreamPosition(from)))
                .withCount(count)
                .withReadDirection(mapDirection(rd))
                .withResolveLinks(rlt)
                .withNoFilter(empty)
                .withUuidOption(uuidOption)
                .withControlOption(s.ReadReq.Options.ControlOption(compatibility = 1))
            )
        )

      for {
        rd <- List(Direction.Forwards, Direction.Backwards)
        fr <- List(sec.StreamPosition.Start, sec.StreamPosition(2200), sec.StreamPosition.End)
        ct <- List(100L, 1000L, 10000L)
        rt <- List(true, false)
      } yield test(rd, fr, ct, rt)
    }

    test("mkReadAllReq") {

      def test(from: LogPosition, rd: Direction, maxCount: Long, rlt: Boolean) =
        assertEquals(
          mkReadAllReq(from, rd, maxCount, rlt),
          s.ReadReq()
            .withOptions(
              s.ReadReq
                .Options()
                .withAll(s.ReadReq.Options.AllOptions(mapLogPosition(from)))
                .withSubscription(s.ReadReq.Options.SubscriptionOptions())
                .withCount(maxCount)
                .withReadDirection(mapDirection(rd))
                .withResolveLinks(rlt)
                .withNoFilter(empty)
                .withUuidOption(uuidOption)
                .withControlOption(s.ReadReq.Options.ControlOption(compatibility = 1))
            )
        )

      for {
        fr <- List(sec.LogPosition.Start, sec.LogPosition.exact(1337L, 1337L), sec.LogPosition.End)
        rd <- List(Direction.Forwards, Direction.Backwards)
        mc <- List(100L, 1000L, 10000L)
        rt <- List(true, false)
      } yield test(fr, rd, mc, rt)
    }

    test("mkDeleteReq") {

      import s.DeleteReq.Options.ExpectedStreamRevision
      val sid = sec.StreamId("abc").unsafe

      def test(ess: StreamState, esr: ExpectedStreamRevision) =
        assertEquals(mkDeleteReq(sid, ess), s.DeleteReq().withOptions(s.DeleteReq.Options(sid.esSid.some, esr)))

      test(sec.StreamPosition(0L), ExpectedStreamRevision.Revision(0L))
      test(sec.StreamState.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(sec.StreamState.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(sec.StreamState.Any, ExpectedStreamRevision.Any(empty))
    }

    test("mkTombstoneReq") {

      import s.TombstoneReq.Options.ExpectedStreamRevision
      val sid = sec.StreamId("abc").unsafe

      def test(ess: StreamState, esr: ExpectedStreamRevision) =
        assertEquals(
          mkTombstoneReq(sid, ess),
          s.TombstoneReq().withOptions(s.TombstoneReq.Options(sid.esSid.some, esr))
        )

      test(sec.StreamPosition(0L), ExpectedStreamRevision.Revision(0L))
      test(sec.StreamState.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(sec.StreamState.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(sec.StreamState.Any, ExpectedStreamRevision.Any(empty))
    }

    test("mkAppendHeaderReq") {

      import s.AppendReq.Options.ExpectedStreamRevision
      val sid = sec.StreamId("abc").unsafe

      def test(ess: StreamState, esr: ExpectedStreamRevision) =
        assertEquals(
          mkAppendHeaderReq(sid, ess),
          s.AppendReq().withOptions(s.AppendReq.Options(sid.esSid.some, esr))
        )

      test(sec.StreamPosition(0L), ExpectedStreamRevision.Revision(0L))
      test(sec.StreamState.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(sec.StreamState.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(sec.StreamState.Any, ExpectedStreamRevision.Any(empty))
    }

    test("mkAppendProposalsReq") {

      import grpc.constants.Metadata.{ContentType, ContentTypes, Type}
      import ContentTypes.{ApplicationJson => Json, ApplicationOctetStream => Binary}

      def json(nr: Int): EventData = {
        val id = JUUID.randomUUID()
        val et = s"et-$nr"
        val da = bv(s"""{ "data" : "$nr" }""")
        val md = bv(s"""{ "meta" : "$nr" }""")
        sec.EventData(et, id, da, md, sec.ContentType.Json).unsafe
      }

      def binary(nr: Int): EventData = {
        val id = JUUID.randomUUID()
        val et = s"et-$nr"
        val da = bv(s"data@$nr")
        val md = bv(s"meta@$nr")
        sec.EventData(et, id, da, md, sec.ContentType.Binary).unsafe
      }

      def test(nel: NonEmptyList[EventData]) = {
        assertEquals(
          mkAppendProposalsReq(nel),
          nel.zipWithIndex.map { case (e, i) =>
            s.AppendReq()
              .withProposedMessage(
                s.AppendReq
                  .ProposedMessage()
                  .withId(mkUuid(e.eventId))
                  .withMetadata(Map(Type -> s"et-$i", ContentType -> e.contentType.fold(Binary, Json)))
                  .withData(e.data.toByteString)
                  .withCustomMetadata(e.metadata.toByteString)
              )
          }
        )
      }

      test(NonEmptyList.of(json(0)))
      test(NonEmptyList.of(json(0), json(1)))
      test(NonEmptyList.of(binary(0)))
      test(NonEmptyList.of(binary(0), binary(1)))
    }

  }

  group("incoming") {

    import incoming._
    import grpc.constants.Metadata.{ContentType, ContentTypes, Created, Type}
    import ContentTypes.{ApplicationJson => Json, ApplicationOctetStream => Binary}

    val empty = Empty()

    test("mkPositionAll") {

      val re     = s.ReadResp.ReadEvent.RecordedEvent()
      val valid1 = re.withStreamRevision(1000L).withCommitPosition(2L).withPreparePosition(2L)
      val valid2 = re.withStreamRevision(-1000L).withCommitPosition(-2L).withPreparePosition(-2L)

      // Happy Path
      assertEquals(
        mkPositionGlobal[ErrorOr](valid1),
        PositionInfo.Global(StreamPosition(1000L), LogPosition.exact(2L, 2L)).asRight
      )

      assertEquals(
        mkPositionGlobal[ErrorOr](valid2),
        PositionInfo.Global(StreamPosition(-1000L), LogPosition.exact(-2L, -2L)).asRight
      )

      // Bad LogPosition
      assertEquals(
        mkPositionGlobal[ErrorOr](valid1.withCommitPosition(-2L).withPreparePosition(-1L)),
        InvalidInput("commit must be >= prepare, but 18446744073709551614 < 18446744073709551615").asLeft
      )
    }

    test("mkStreamPosition") {

      val re = s.ReadResp.ReadEvent.RecordedEvent()

      // Happy Path
      assertEquals(mkStreamPosition[ErrorOr](re.withStreamRevision(1L)), StreamPosition(1L).asRight)
      assertEquals(mkStreamPosition[ErrorOr](re.withStreamRevision(-1L)), StreamPosition.Exact(ULong.max).asRight)

    }

    test("mkEvent") {

      def test[P <: PositionInfo](
        er: EventRecord[P],
        lr: EventRecord[P],
        eventProto: s.ReadResp.ReadEvent.RecordedEvent,
        linkProto: s.ReadResp.ReadEvent.RecordedEvent,
        mkPos: s.ReadResp.ReadEvent.RecordedEvent => ErrorOr[P]
      ) = {

        val readEvent = s.ReadResp.ReadEvent()

        // Event & No Link => EventRecord
        assertEquals(mkEvent[ErrorOr, P](readEvent.withEvent(eventProto), mkPos), er.some.asRight)

        // Event & Link => ResolvedEvent
        assertEquals(mkEvent[ErrorOr, P](readEvent.withEvent(eventProto).withLink(linkProto), mkPos),
                     ResolvedEvent(er, lr).some.asRight)

        // No Event & No Link => None
        assertEquals(mkEvent[ErrorOr, P](readEvent, mkPos), Option.empty[Event[P]].asRight)

        // No Event & Link, i.e. link to deleted event => None
        assertEquals(mkEvent[ErrorOr, P](readEvent.withLink(linkProto), mkPos), Option.empty[Event[P]].asRight)

        // Require read event
        assertEquals(reqReadEvent[ErrorOr, P](s.ReadResp().withEvent(readEvent.withEvent(eventProto)), mkPos),
                     er.some.asRight)

        assertEquals(reqReadEvent[ErrorOr, P](s.ReadResp(), mkPos),
                     ProtoResultError("Required value ReadEvent missing or invalid.").asLeft)

      }

      //

      val sel = mkStreamEventAndLink

      test(sel.event, sel.link, sel.eventProto, sel.linkProto, mkStreamPosition[ErrorOr])

      val ael = mkAllEventAndLink

      test(ael.event, ael.link, ael.eventProto, ael.linkProto, mkPositionGlobal[ErrorOr])

    }

    test("mkEventRecord") {

      val streamId        = "abc"
      val revision        = 1L
      val commit          = 1L
      val prepare         = 1L
      val id              = "e5390fcb-48bd-4895-bcc3-01629cca2af6"
      val eventType       = "eventtype"
      val dataValue       = "data"
      val data            = ByteVector.encodeUtf8(dataValue).leftMap(_.getMessage()).unsafe
      val customMetaValue = "meta"
      val customMeta      = ByteVector.encodeUtf8(customMetaValue).leftMap(_.getMessage()).unsafe
      val created         = Instant.EPOCH.atZone(ZoneOffset.UTC)
      val metadata        = Map(ContentType -> Binary, Type -> eventType, Created -> created.getNano().toString)

      def test[P <: PositionInfo](
        eventRecord: EventRecord[P],
        recordedEvent: s.ReadResp.ReadEvent.RecordedEvent,
        mkPos: s.ReadResp.ReadEvent.RecordedEvent => ErrorOr[P]
      ) = {

        // Happy Path
        assertEquals(mkEventRecord[ErrorOr, P](recordedEvent, mkPos), eventRecord.asRight)

        // Bad StreamId
        assertEquals(
          mkEventRecord[ErrorOr, P](recordedEvent.withStreamIdentifier("".toStreamIdentifer), mkPos),
          ProtoResultError("name cannot be empty").asLeft
        )

        // Missing UUID
        assertEquals(
          mkEventRecord[ErrorOr, P](recordedEvent.withId(UUID().withValue(UUID.Value.Empty)), mkPos),
          ProtoResultError("UUID is missing").asLeft
        )

        // Bad UUID
        assertEquals(
          mkEventRecord[ErrorOr, P](recordedEvent.withId(UUID().withString("invalid")), mkPos),
          ProtoResultError("Invalid UUID string: invalid").asLeft
        )

        // Missing EventType
        assertEquals(
          mkEventRecord[ErrorOr, P](
            recordedEvent.withMetadata(metadata.view.filterKeys(_ != Type).toMap),
            mkPos
          ),
          ProtoResultError(s"Required value $Type missing or invalid.").asLeft
        )

        // Missing ContentType
        assertEquals(
          mkEventRecord[ErrorOr, P](
            recordedEvent.withMetadata(metadata.view.filterKeys(_ != ContentType).toMap),
            mkPos
          ),
          ProtoResultError(s"Required value $ContentType missing or invalid.").asLeft
        )

        // Bad ContentType
        assertEquals(
          mkEventRecord[ErrorOr, P](recordedEvent.withMetadata(metadata.updated(ContentType, "no")), mkPos),
          ProtoResultError(s"Required value $ContentType missing or invalid: no").asLeft
        )

        // Missing Created
        assertEquals(
          mkEventRecord[ErrorOr, P](
            recordedEvent.withMetadata(metadata.view.filterKeys(_ != Created).toMap),
            mkPos
          ),
          ProtoResultError(s"Required value $Created missing or invalid.").asLeft
        )

        // Bad Created
        assertEquals(
          mkEventRecord[ErrorOr, P](
            recordedEvent.withMetadata(metadata.updated(Created, "chuck norris")),
            mkPos
          ),
          ProtoResultError(s"Required value $Created missing or invalid.").asLeft
        )
      }

      // /

      val sid = sec.StreamId(streamId).unsafe
      val et  = sec.EventType(eventType).unsafe
      val sp  = sec.StreamPosition(revision)
      val ed  = sec.EventData(et, JUUID.fromString(id), data, customMeta, sec.ContentType.Binary)

      val streamRecordedEvent =
        s.ReadResp.ReadEvent
          .RecordedEvent()
          .withStreamIdentifier(streamId.toStreamIdentifer)
          .withStreamRevision(revision)
          .withData(data.toByteString)
          .withCustomMetadata(customMeta.toByteString)
          .withId(UUID().withString(id))
          .withMetadata(metadata)

      val streamEventRecord =
        sec.EventRecord(sid, sp, ed, created)

      test(streamEventRecord, streamRecordedEvent, mkStreamPosition[ErrorOr])

      val allRecordedEvent =
        streamRecordedEvent.withCommitPosition(commit).withPreparePosition(prepare)

      val allEventRecord =
        sec.EventRecord(sid, sec.PositionInfo.Global(sp, sec.LogPosition.exact(commit, prepare)), ed, created)

      test(allEventRecord, allRecordedEvent, mkPositionGlobal[ErrorOr])

    }

    test("mkCheckpoint") {

      assertEquals(
        mkCheckpoint[ErrorOr](s.ReadResp.Checkpoint(1L, 1L)),
        Checkpoint(sec.LogPosition.exact(1L, 1L)).asRight
      )

      assertEquals(
        mkCheckpoint[ErrorOr](s.ReadResp.Checkpoint(-1L, 0L)),
        Checkpoint(sec.LogPosition.Exact.create(ULong.max, ULong.min)).asRight
      )
    }

    test("mkCheckpointOrEvent") {

      val created     = Instant.EPOCH.atZone(ZoneOffset.UTC)
      val event       = sampleOfGen(eventGen.allEventRecordOne).copy(created = created)
      val eventData   = event.eventData
      val eventType   = sec.EventType.eventTypeToString(event.eventData.eventType)
      val contentType = eventData.contentType.fold(Binary, Json)
      val metadata    = Map(ContentType -> contentType, Type -> eventType, Created -> created.getNano().toString)

      val recordedEvent = s.ReadResp.ReadEvent
        .RecordedEvent()
        .withStreamIdentifier(event.streamId.stringValue.toStreamIdentifer)
        .withStreamRevision(event.streamPosition.value.toLong)
        .withCommitPosition(event.logPosition.commit.toLong)
        .withPreparePosition(event.logPosition.prepare.toLong)
        .withData(event.eventData.data.toByteString)
        .withCustomMetadata(event.eventData.metadata.toByteString)
        .withId(UUID().withString(event.eventData.eventId.toString))
        .withMetadata(metadata)

      val checkpoint = s.ReadResp.Checkpoint(1L, 1L)

      assertEquals(
        mkCheckpointOrEvent[ErrorOr](s.ReadResp().withEvent(s.ReadResp.ReadEvent().withEvent(recordedEvent))),
        Some(event.asRight[Checkpoint]).asRight
      )

      assertEquals(
        mkCheckpointOrEvent[ErrorOr](s.ReadResp().withCheckpoint(checkpoint)),
        Some(Checkpoint(sec.LogPosition.exact(1L, 1L)).asLeft[AllEvent]).asRight
      )

      assertEquals(
        mkCheckpointOrEvent[ErrorOr](s.ReadResp()),
        Option.empty[Either[Checkpoint, AllEvent]].asRight
      )
    }

    test("mkStreamNotFound") {

      val sn  = s.ReadResp.StreamNotFound()
      val sni = sn.withStreamIdentifier("abc".toStreamIdentifer)

      assertEquals(
        mkStreamNotFound[ErrorOr](sn),
        ProtoResultError("Required value StreamIdentifer missing or invalid.").asLeft
      )

      assertEquals(mkStreamNotFound[ErrorOr](sni), StreamNotFound("abc").asRight)

    }

    test("failStreamNotFound") {

      val rr = s.ReadResp()
      val cp = rr.withCheckpoint(s.ReadResp.Checkpoint(1L, 1L))
      val sn = rr.withStreamNotFound(s.ReadResp.StreamNotFound().withStreamIdentifier("abc".toStreamIdentifer))

      assertEquals(failStreamNotFound[ErrorOr](sn), StreamNotFound("abc").asLeft)
      assertEquals(failStreamNotFound[ErrorOr](cp), cp.asRight)

    }

    test("reqConfirmation") {

      assertEquals(
        reqConfirmation[ErrorOr](s.ReadResp()),
        ProtoResultError("Required value SubscriptionConfirmation missing or invalid. Got Empty instead").asLeft
      )

      assertEquals(
        reqConfirmation[ErrorOr](s.ReadResp().withConfirmation(s.ReadResp.SubscriptionConfirmation("id"))),
        SubscriptionConfirmation("id").asRight
      )

    }

    test("mkEventType") {
      assertEquals(mkEventType[ErrorOr](null), ProtoResultError("Event type name cannot be empty").asLeft)
      assertEquals(mkEventType[ErrorOr](""), ProtoResultError("Event type name cannot be empty").asLeft)
      assertEquals(mkEventType[ErrorOr]("sec.protos.A"), sec.EventType.normal("sec.protos.A").unsafe.asRight)
      assertEquals(mkEventType[ErrorOr]("$system-type"), sec.EventType.system("system-type").unsafe.asRight)
      assertEquals(mkEventType[ErrorOr]("$>"), sec.EventType.LinkTo.asRight)
    }

    test("mkWriteResult") {

      import s.AppendResp.{Result, Success}
      import s.AppendResp.WrongExpectedVersion.ExpectedRevisionOption
      import s.AppendResp.WrongExpectedVersion.CurrentRevisionOption

      val sid: StreamId.Id                           = sec.StreamId("abc").unsafe
      val test: s.AppendResp => ErrorOr[WriteResult] = mkWriteResult[ErrorOr](sid, _)

      val successRevOne   = Success().withCurrentRevision(1L).withPosition(s.AppendResp.Position(1L, 1L))
      val successRevEmpty = Success().withCurrentRevisionOption(Success.CurrentRevisionOption.Empty)
      val successNoStream = Success().withNoStream(empty)

      assertEquals(
        test(s.AppendResp().withSuccess(successRevOne)),
        WriteResult(sec.StreamPosition(1L), sec.LogPosition.exact(1L, 1L)).asRight
      )

      assertEquals(
        test(s.AppendResp().withSuccess(successNoStream)),
        ProtoResultError("Did not expect NoStream when using NonEmptyList").asLeft
      )

      assertEquals(
        test(s.AppendResp().withSuccess(successRevEmpty)),
        ProtoResultError("CurrentRevisionOption is missing").asLeft
      )

      assertEquals(
        test(s.AppendResp().withSuccess(successRevOne.withNoPosition(empty))),
        ProtoResultError("Did not expect NoPosition when using NonEmptyList").asLeft
      )

      assertEquals(
        test(s.AppendResp().withSuccess(successRevOne.withPositionOption(Success.PositionOption.Empty))),
        ProtoResultError("PositionOption is missing").asLeft
      )

      //

      val wreExpectedOne          = ExpectedRevisionOption.ExpectedRevision(1L)
      val wreExpectedNoStream     = ExpectedRevisionOption.ExpectedNoStream(empty)
      val wreExpectedAny          = ExpectedRevisionOption.ExpectedAny(empty)
      val wreExpectedStreamExists = ExpectedRevisionOption.ExpectedStreamExists(empty)
      val wreExpectedEmpty        = ExpectedRevisionOption.Empty

      val wreCurrentRevTwo   = CurrentRevisionOption.CurrentRevision(2L)
      val wreCurrentNoStream = CurrentRevisionOption.CurrentNoStream(empty)
      val wreCurrentEmpty    = CurrentRevisionOption.Empty

      def mkExpected(e: ExpectedRevisionOption) = s
        .AppendResp()
        .withWrongExpectedVersion(
          s.AppendResp.WrongExpectedVersion(expectedRevisionOption = e, currentRevisionOption = wreCurrentRevTwo)
        )

      def testExpected(ero: ExpectedRevisionOption, expected: StreamState) =
        assertEquals(test(mkExpected(ero)), WrongExpectedState(sid, expected, sec.StreamPosition(2L)).asLeft)

      testExpected(wreExpectedOne, sec.StreamPosition(1L))
      testExpected(wreExpectedNoStream, sec.StreamState.NoStream)
      testExpected(wreExpectedAny, sec.StreamState.Any)
      testExpected(wreExpectedStreamExists, sec.StreamState.StreamExists)

      assertEquals(test(mkExpected(wreExpectedEmpty)), ProtoResultError("ExpectedRevisionOption is missing").asLeft)

      def mkCurrent(c: CurrentRevisionOption) = s
        .AppendResp()
        .withWrongExpectedVersion(
          s.AppendResp.WrongExpectedVersion(expectedRevisionOption = wreExpectedOne, currentRevisionOption = c)
        )

      def testCurrent(cro: CurrentRevisionOption, actual: StreamState) =
        assertEquals(test(mkCurrent(cro)), WrongExpectedState(sid, sec.StreamPosition(1L), actual).asLeft)

      testCurrent(wreCurrentRevTwo, sec.StreamPosition(2L))
      testCurrent(wreCurrentNoStream, sec.StreamState.NoStream)
      assertEquals(test(mkCurrent(wreCurrentEmpty)), ProtoResultError("CurrentRevisionOption is missing").asLeft)

      //

      assertEquals(test(s.AppendResp().withResult(Result.Empty)), ProtoResultError("Result is missing").asLeft)

    }

    test("mkDeleteResult") {
      assertEquals(
        mkDeleteResult[ErrorOr](s.DeleteResp().withPosition(s.DeleteResp.Position(1L, 1L))),
        DeleteResult(sec.LogPosition.exact(1L, 1L)).asRight
      )

      assertEquals(
        mkDeleteResult[ErrorOr](s.DeleteResp().withNoPosition(empty)),
        ProtoResultError("Required value DeleteResp.PositionOptions.Position missing or invalid.").asLeft
      )

    }

    test("mkTombstoneResult") {
      assertEquals(
        mkTombstoneResult[ErrorOr](s.TombstoneResp().withPosition(s.TombstoneResp.Position(1L, 1L))),
        TombstoneResult(sec.LogPosition.exact(1L, 1L)).asRight
      )

      assertEquals(
        mkTombstoneResult[ErrorOr](s.TombstoneResp().withNoPosition(empty)),
        ProtoResultError("Required value TombstoneResp.PositionOptions.Position missing or invalid.").asLeft
      )
    }

    test("mkStreamMessageNotFound") {

      val sn  = s.ReadResp.StreamNotFound()
      val sni = sn.withStreamIdentifier("abc".toStreamIdentifer)

      assertEquals(
        mkStreamMessageNotFound[ErrorOr](sn),
        ProtoResultError("Required value StreamIdentifer missing or invalid.").asLeft
      )

      assertEquals(
        mkStreamMessageNotFound[ErrorOr](sni),
        StreamMessage.NotFound(StreamId("abc").unsafe).asRight
      )
    }

    test("mkStreamMessageEvent") {

      val created     = Instant.EPOCH.atZone(ZoneOffset.UTC)
      val event       = sampleOfGen(eventGen.streamEventRecordOne).copy(created = created)
      val eventData   = event.eventData
      val eventType   = sec.EventType.eventTypeToString(event.eventData.eventType)
      val contentType = eventData.contentType.fold(Binary, Json)
      val metadata    = Map(ContentType -> contentType, Type -> eventType, Created -> created.getNano().toString)

      val recordedEvent = s.ReadResp.ReadEvent
        .RecordedEvent()
        .withStreamIdentifier(event.streamId.stringValue.toStreamIdentifer)
        .withStreamRevision(event.streamPosition.value.toLong)
        .withData(event.eventData.data.toByteString)
        .withCustomMetadata(event.eventData.metadata.toByteString)
        .withId(UUID().withString(event.eventData.eventId.toString))
        .withMetadata(metadata)

      // Sanity checks, see mkEvent for more coverage.

      assertEquals(
        mkStreamMessageEvent[ErrorOr](s.ReadResp.ReadEvent().withEvent(recordedEvent)),
        StreamMessage.Event(event).some.asRight
      )

      assertEquals(
        mkStreamMessageEvent[ErrorOr](s.ReadResp.ReadEvent()),
        Option.empty[StreamMessage.Event].asRight
      )
    }

    property("mkStreamMessageFirst") {
      forAll { (v: Long) =>
        assertEquals(mkStreamMessageFirst[ErrorOr](v), StreamMessage.FirstStreamPosition(StreamPosition(v)).asRight)
      }
    }

    property("mkStreamMessageLast") {
      forAll { (v: Long) =>
        assertEquals(mkStreamMessageLast[ErrorOr](v), StreamMessage.LastStreamPosition(StreamPosition(v)).asRight)
      }
    }

    test("mkAllMessageEvent") {

      val ael = mkAllEventAndLink

      assertEquals(
        mkAllMessageEvent[ErrorOr](s.ReadResp.ReadEvent().withEvent(ael.eventProto)),
        AllMessage.Event(ael.event).some.asRight
      )

      assertEquals(
        mkAllMessageEvent[ErrorOr](s.ReadResp.ReadEvent()),
        Option.empty[AllMessage.Event].asRight
      )
    }

    property("mkAllMessageLast") {

      val p1 = forAll { (c: ULong, p: ULong) =>
        (p <= c) ==> {
          mkAllMessageLast[ErrorOr](AllStreamPosition(c.toLong, p.toLong)) ==
            AllMessage.LastAllStreamPosition(LogPosition.exact(c.toLong, p.toLong)).asRight
        }
      }

      val p2 = forAll { (c: ULong, p: ULong) =>
        (p > c) ==> {
          mkAllMessageLast[ErrorOr](AllStreamPosition(c.toLong, p.toLong)) ==
            InvalidInput(s"commit must be >= prepare, but $c < $p").asLeft
        }
      }

      p1 && p2

    }

    group("AllResult.fromWire") {

      val rsp = s.ReadResp()
      val run = AllResult.fromWire[ErrorOr] _

      /** Empty */

      test("Empty") {
        assertEquals(run(rsp), ProtoResultError(s"Unexpected response for AllResult: Empty").asLeft)
      }

      /** Confirmation */
      property("Confirmation") {
        forAll { (id: String) =>
          run(rsp.withConfirmation(s.ReadResp.SubscriptionConfirmation(id))) ==
            AllResult.ConfirmationR(SubscriptionConfirmation(id)).asRight
        }
      }

      /** CheckpointR */

      property("CheckpointR") {

        val valid = forAll { (c: ULong, p: ULong) =>
          (p <= c) ==> {
            run(rsp.withCheckpoint(s.ReadResp.Checkpoint(c.toLong, p.toLong))) ==
              AllResult.CheckpointR(Checkpoint(sec.LogPosition.exact(c.toLong, p.toLong))).asRight
          }
        }

        val invalid = forAll { (c: ULong, p: ULong) =>
          (p > c) ==> {
            run(rsp.withCheckpoint(s.ReadResp.Checkpoint(c.toLong, p.toLong))) ==
              ProtoResultError(s"Invalid position for Checkpoint: commit must be >= prepare, but $c < $p").asLeft
          }
        }

        valid && invalid
      }

      property("LastPositionR") {

        val valid = forAll { (c: ULong, p: ULong) =>
          (p <= c) ==> {
            run(rsp.withLastAllStreamPosition(AllStreamPosition(c.toLong, p.toLong))) ==
              AllResult.LastPositionR(LogPosition.exact(c.toLong, p.toLong)).asRight
          }
        }

        val invalid = forAll { (c: ULong, p: ULong) =>
          (p > c) ==> {
            run(rsp.withLastAllStreamPosition(AllStreamPosition(c.toLong, p.toLong))) ==
              ProtoResultError(s"commit must be >= prepare, but $c < $p").asLeft
          }
        }

        valid && invalid
      }

      /** EventR */

      val ael = mkAllEventAndLink
      val re  = s.ReadResp.ReadEvent()

      test("Event & No Link => EventRecord") {
        assertEquals(
          run(rsp.withEvent(re.withEvent(ael.eventProto))),
          AllResult.EventR(ael.event.some).asRight
        )
      }

      test("Event & Link => ResolvedEvent") {
        assertEquals(
          run(rsp.withEvent(re.withEvent(ael.eventProto).withLink(ael.linkProto))),
          AllResult.EventR(ResolvedEvent(ael.event, ael.link).some).asRight
        )
      }

      test("No Event & No Link => None") {
        assertEquals(
          run(rsp.withEvent(re)),
          AllResult.EventR(None).asRight
        )
      }

      test("No Event & Link, i.e. link to deleted event => None") {
        assertEquals(
          run(rsp.withEvent(re.withLink(ael.linkProto))),
          AllResult.EventR(None).asRight
        )
      }

    }
  }

}

object StreamsMappingSuite {

  import grpc.constants.Metadata.{ContentType, ContentTypes, Created, Type}
  import ContentTypes.{ApplicationJson => Json, ApplicationOctetStream => Binary}

  def bv(data: String): ByteVector =
    encodeToBV(data).unsafe

  final case class EventAndLink[P <: PositionInfo](
    event: EventRecord[P],
    eventProto: s.ReadResp.ReadEvent.RecordedEvent,
    link: EventRecord[P],
    linkProto: s.ReadResp.ReadEvent.RecordedEvent
  )

  def mkStreamEventAndLink: EventAndLink[PositionInfo.Local] =
    mkEventAndLink[PositionInfo.Local]((sp, _, _) => sp)

  def mkAllEventAndLink: EventAndLink[PositionInfo.Global] =
    mkEventAndLink[PositionInfo.Global]((sp, c, p) => sec.PositionInfo.Global(sp, sec.LogPosition.exact(c, p)))

  private def mkEventAndLink[P <: PositionInfo](
    mkPosition: (StreamPosition.Exact, Long, Long) => P
  ): EventAndLink[P] = {

    val streamId   = "abc-3"
    val revision   = 1L
    val commit     = 1L
    val prepare    = 1L
    val id         = "e5390fcb-48bd-4895-bcc3-01629cca2af6"
    val eventType  = "eventtype"
    val data       = ByteVector.encodeUtf8("""{ "data": "data" }""").leftMap(_.getMessage()).unsafe
    val customMeta = ByteVector.empty
    val created    = Instant.EPOCH.atZone(ZoneOffset.UTC)
    val metadata   = Map(ContentType -> Json, Type -> eventType, Created -> created.getNano().toString)

    val eventProto =
      s.ReadResp.ReadEvent
        .RecordedEvent()
        .withStreamIdentifier(streamId.toStreamIdentifer)
        .withStreamRevision(revision)
        .withCommitPosition(commit)
        .withPreparePosition(prepare)
        .withData(data.toByteString)
        .withCustomMetadata(customMeta.toByteString)
        .withId(UUID().withString(id))
        .withMetadata(metadata)

    val linkStreamId   = "abc"
    val linkRevision   = 2L
    val linkCommit     = 10L
    val linkPrepare    = 10L
    val linkId         = "b8f5ed88-5aa1-49a6-85d6-c173556436ae"
    val linkEventType  = EventType.LinkTo.stringValue
    val linkData       = bv(s"$revision@$streamId")
    val linkCustomMeta = ByteVector.empty
    val linkCreated    = Instant.EPOCH.atZone(ZoneOffset.UTC)
    val linkMetadata   = Map(ContentType -> Binary, Type -> linkEventType, Created -> linkCreated.getNano().toString)

    val linkProto = s.ReadResp.ReadEvent
      .RecordedEvent()
      .withStreamIdentifier(linkStreamId.toStreamIdentifer)
      .withStreamRevision(linkRevision)
      .withCommitPosition(linkCommit)
      .withPreparePosition(linkPrepare)
      .withData(linkData.toByteString)
      .withCustomMetadata(linkCustomMeta.toByteString)
      .withId(UUID().withString(linkId))
      .withMetadata(linkMetadata)

    val sid = sec.StreamId(streamId).unsafe
    val sp  = sec.StreamPosition(revision)
    val et  = sec.EventType(eventType).unsafe
    val ed  = sec.EventData(et, JUUID.fromString(id), data, customMeta, sec.ContentType.Json)

    val lsid = sec.StreamId(linkStreamId).unsafe
    val lsp  = sec.StreamPosition(linkRevision)
    val let  = sec.EventType.LinkTo
    val led  = sec.EventData(let, JUUID.fromString(linkId), linkData, linkCustomMeta, sec.ContentType.Binary)

    val eventRecord = EventRecord(sid, mkPosition(sp, commit, prepare), ed, created)
    val linkRecord  = sec.EventRecord(lsid, mkPosition(lsp, linkCommit, linkPrepare), led, linkCreated)

    EventAndLink(eventRecord, eventProto, linkRecord, linkProto)

  }

}
