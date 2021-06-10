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
import com.eventstore.client.{Empty, FilterOptions, UUID}
import com.eventstore.client.{streams => s}
import org.specs2._
import scodec.bits.ByteVector
import sec.api.exceptions.{StreamNotFound, WrongExpectedState}
import sec.api.mapping.implicits._
import sec.api.mapping.shared._
import sec.api.mapping.streams.{incoming, outgoing}
import sec.helpers.implicits._
import sec.helpers.text.encodeToBV

class StreamsMappingSpec extends mutable.Specification {

  def bv(data: String): ByteVector =
    encodeToBV(data).unsafe

  "outgoing" >> {

    import outgoing._
    import s.ReadReq.Options.AllOptions.AllOption
    import s.ReadReq.Options.StreamOptions.RevisionOption

    val empty = Empty()

    ///

    "uuidOption" >> {
      uuidOption shouldEqual s.ReadReq.Options.UUIDOption().withStructured(empty)
    }

    "LogPosition" >> {
      mapLogPosition(sec.LogPosition.exact(1L, 2L)) shouldEqual AllOption.Position(s.ReadReq.Options.Position(1L, 2L))
      mapLogPosition(sec.LogPosition.End) shouldEqual AllOption.End(empty)
      mapLogPositionOpt(sec.LogPosition.exact(0L, 0L).some) shouldEqual AllOption.Position(
        s.ReadReq.Options.Position(0L, 0L))
      mapLogPositionOpt(None) shouldEqual AllOption.Start(empty)
    }

    "mapStreamPosition" >> {
      mapStreamPosition(sec.StreamPosition.exact(1L)) shouldEqual RevisionOption.Revision(1L)
      mapStreamPosition(sec.StreamPosition.End) shouldEqual RevisionOption.End(empty)
      mapStreamPositionOpt(sec.StreamPosition.exact(0L).some) shouldEqual RevisionOption.Revision(0L)
      mapStreamPositionOpt(None) shouldEqual RevisionOption.Start(empty)
    }

    "mapDirection" >> {
      mapDirection(Direction.Forwards) shouldEqual s.ReadReq.Options.ReadDirection.Forwards
      mapDirection(Direction.Backwards) shouldEqual s.ReadReq.Options.ReadDirection.Backwards
    }

    "mapReadEventFilter" >> {

      import EventFilter._
      import s.ReadReq.Options.FilterOption.{Filter, NoFilter}
      import FilterOptions.Expression

      def mkOptions(f: EventFilter, maxWindow: Option[Int] = 10.some, multiplier: Int = 1) =
        SubscriptionFilterOptions(f, maxWindow, multiplier).some

      mapReadEventFilter(None) shouldEqual NoFilter(empty)

      mapReadEventFilter(mkOptions(streamIdPrefix("a", "b"))) shouldEqual Filter(
        FilterOptions()
          .withStreamName(Expression().withPrefix(List("a", "b")))
          .withMax(10)
          .withCheckpointIntervalMultiplier(1)
      )

      mapReadEventFilter(mkOptions(streamIdPrefix("a"), None, 1)) shouldEqual Filter(
        FilterOptions()
          .withStreamName(Expression().withPrefix(List("a")))
          .withCount(empty)
          .withCheckpointIntervalMultiplier(1)
      )

      mapReadEventFilter(mkOptions(eventTypePrefix("a", "b"))) shouldEqual Filter(
        FilterOptions()
          .withEventType(Expression().withPrefix(List("a", "b")))
          .withMax(10)
          .withCheckpointIntervalMultiplier(1)
      )

      mapReadEventFilter(mkOptions(eventTypePrefix("a"), None, 1)) shouldEqual Filter(
        FilterOptions()
          .withEventType(Expression().withPrefix(List("a")))
          .withCount(empty)
          .withCheckpointIntervalMultiplier(1)
      )

      mapReadEventFilter(mkOptions(streamIdRegex("^[^$].*"))) shouldEqual Filter(
        FilterOptions()
          .withStreamName(Expression().withRegex("^[^$].*"))
          .withMax(10)
          .withCheckpointIntervalMultiplier(1)
      )

      mapReadEventFilter(mkOptions(streamIdRegex("^(ns_).+"), None, 1)) shouldEqual Filter(
        FilterOptions()
          .withStreamName(Expression().withRegex("^(ns_).+"))
          .withCount(empty)
          .withCheckpointIntervalMultiplier(1)
      )

      mapReadEventFilter(mkOptions(eventTypeRegex("^[^$].*"))) shouldEqual Filter(
        FilterOptions()
          .withEventType(Expression().withRegex("^[^$].*"))
          .withMax(10)
          .withCheckpointIntervalMultiplier(1)
      )

      mapReadEventFilter(mkOptions(eventTypeRegex("^(ns_).+"), None, 1)) shouldEqual Filter(
        FilterOptions()
          .withEventType(Expression().withRegex("^(ns_).+"))
          .withCount(empty)
          .withCheckpointIntervalMultiplier(1)
      )

    }

    "mkSubscribeToStreamReq" >> {
      import StreamPosition._

      val sid = StreamId("abc").unsafe

      def test(exclusiveFrom: Option[StreamPosition], resolveLinkTos: Boolean) =
        mkSubscribeToStreamReq(sid, exclusiveFrom, resolveLinkTos) shouldEqual s
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

      for {
        ef <- List(Option.empty[StreamPosition], Start.some, exact(1337L).some, End.some)
        rt <- List(true, false)
      } yield test(ef, rt)
    }

    "mkSubscribeToAllReq" >> {

      import LogPosition._
      import EventFilter._

      def test(exclusiveFrom: Option[LogPosition], resolveLinkTos: Boolean, filter: Option[SubscriptionFilterOptions]) =
        mkSubscribeToAllReq(exclusiveFrom, resolveLinkTos, filter) shouldEqual s
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

    "mkReadStreamReq" >> {

      val sid = sec.StreamId("abc").unsafe

      def test(rd: Direction, from: StreamPosition, count: Long, rlt: Boolean) =
        mkReadStreamReq(sid, from, rd, count, rlt) shouldEqual s
          .ReadReq()
          .withOptions(
            s.ReadReq
              .Options()
              .withStream(s.ReadReq.Options.StreamOptions(sid.esSid.some, mapStreamPosition(from)))
              .withCount(count)
              .withReadDirection(mapDirection(rd))
              .withResolveLinks(rlt)
              .withNoFilter(empty)
              .withUuidOption(uuidOption)
          )

      for {
        rd <- List(Direction.Forwards, Direction.Backwards)
        fr <- List(sec.StreamPosition.Start, sec.StreamPosition.exact(2200), sec.StreamPosition.End)
        ct <- List(100L, 1000L, 10000L)
        rt <- List(true, false)
      } yield test(rd, fr, ct, rt)
    }

    "mkReadAllReq" >> {

      def test(from: LogPosition, rd: Direction, maxCount: Long, rlt: Boolean) =
        mkReadAllReq(from, rd, maxCount, rlt) shouldEqual s
          .ReadReq()
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
          )

      for {
        fr <- List(sec.LogPosition.Start, sec.LogPosition.exact(1337L, 1337L), sec.LogPosition.End)
        rd <- List(Direction.Forwards, Direction.Backwards)
        mc <- List(100L, 1000L, 10000L)
        rt <- List(true, false)
      } yield test(fr, rd, mc, rt)
    }

    "mkDeleteReq" >> {

      import s.DeleteReq.Options.ExpectedStreamRevision
      val sid = sec.StreamId("abc").unsafe

      def test(ess: StreamState, esr: ExpectedStreamRevision) =
        mkDeleteReq(sid, ess) shouldEqual s.DeleteReq().withOptions(s.DeleteReq.Options(sid.esSid.some, esr))

      test(sec.StreamPosition.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(sec.StreamState.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(sec.StreamState.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(sec.StreamState.Any, ExpectedStreamRevision.Any(empty))
    }

    "mkTombstoneReq" >> {

      import s.TombstoneReq.Options.ExpectedStreamRevision
      val sid = sec.StreamId("abc").unsafe

      def test(ess: StreamState, esr: ExpectedStreamRevision) =
        mkTombstoneReq(sid, ess) shouldEqual s.TombstoneReq().withOptions(s.TombstoneReq.Options(sid.esSid.some, esr))

      test(sec.StreamPosition.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(sec.StreamState.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(sec.StreamState.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(sec.StreamState.Any, ExpectedStreamRevision.Any(empty))
    }

    "mkAppendHeaderReq" >> {

      import s.AppendReq.Options.ExpectedStreamRevision
      val sid = sec.StreamId("abc").unsafe

      def test(ess: StreamState, esr: ExpectedStreamRevision) =
        mkAppendHeaderReq(sid, ess) shouldEqual s.AppendReq().withOptions(s.AppendReq.Options(sid.esSid.some, esr))

      test(sec.StreamPosition.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(sec.StreamState.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(sec.StreamState.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(sec.StreamState.Any, ExpectedStreamRevision.Any(empty))
    }

    "mkAppendProposalsReq" >> {

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
        mkAppendProposalsReq(nel) shouldEqual nel.zipWithIndex.map { case (e, i) =>
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
      }

      test(NonEmptyList.of(json(0)))
      test(NonEmptyList.of(json(0), json(1)))
      test(NonEmptyList.of(binary(0)))
      test(NonEmptyList.of(binary(0), binary(1)))
    }

  }

  "incoming" >> {

    import incoming._
    import grpc.constants.Metadata.{ContentType, ContentTypes, Created, Type}
    import ContentTypes.{ApplicationJson => Json, ApplicationOctetStream => Binary}

    val empty = Empty()

    "mkPositionAll" >> {

      val re    = s.ReadResp.ReadEvent.RecordedEvent()
      val valid = re.withStreamRevision(1L).withCommitPosition(2L).withPreparePosition(2L)

      // Happy Path
      mkPositionGlobal[ErrorOr](valid) shouldEqual
        PositionInfo.Global(StreamPosition.exact(1L), LogPosition.exact(2L, 2L)).asRight

      // Bad StreamPosition
      mkPositionGlobal[ErrorOr](valid.withStreamRevision(-1L)) shouldEqual
        InvalidInput("value must be >= 0, but is -1").asLeft

      // Bad LogPosition
      mkPositionGlobal[ErrorOr](valid.withCommitPosition(-1L).withPreparePosition(-1L)) shouldEqual
        InvalidInput("commit must be >= 0, but is -1").asLeft

    }

    "mkStreamPosition" >> {

      val re    = s.ReadResp.ReadEvent.RecordedEvent()
      val valid = re.withStreamRevision(1L)

      // Happy Path
      mkStreamPosition[ErrorOr](valid) shouldEqual
        StreamPosition.exact(1L).asRight

      // Invalid
      mkStreamPosition[ErrorOr](valid.withStreamRevision(-2L)) shouldEqual
        InvalidInput("value must be >= 0, but is -2").asLeft
    }

    "mkEvent" >> {

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

      def test[P <: PositionInfo](
        er: EventRecord[P],
        lr: EventRecord[P],
        mkPos: s.ReadResp.ReadEvent.RecordedEvent => ErrorOr[P]
      ) = {

        ///

        val readEvent = s.ReadResp.ReadEvent()

        // Event & No Link => EventRecord
        mkEvent[ErrorOr, P](readEvent.withEvent(eventProto), mkPos) shouldEqual er.some.asRight

        // Event & Link => ResolvedEvent
        mkEvent[ErrorOr, P](readEvent.withEvent(eventProto).withLink(linkProto), mkPos) shouldEqual
          ResolvedEvent(er, lr).some.asRight

        // No Event & No Link => None
        mkEvent[ErrorOr, P](readEvent, mkPos) shouldEqual Option.empty[Event[P]].asRight

        // No Event & Link, i.e. link to deleted event => None
        mkEvent[ErrorOr, P](readEvent.withLink(linkProto), mkPos) shouldEqual Option.empty[Event[P]].asRight

        // Require read event
        reqReadEvent[ErrorOr, P](s.ReadResp().withEvent(readEvent.withEvent(eventProto)), mkPos) shouldEqual
          er.some.asRight

        reqReadEvent[ErrorOr, P](s.ReadResp(), mkPos) shouldEqual
          ProtoResultError("Required value ReadEvent missing or invalid.").asLeft

      }

      val sid = sec.StreamId(streamId).unsafe
      val sp  = sec.StreamPosition.exact(revision)
      val et  = sec.EventType(eventType).unsafe
      val ed  = sec.EventData(et, JUUID.fromString(id), data, customMeta, sec.ContentType.Json)

      val lsid = sec.StreamId(linkStreamId).unsafe
      val lsp  = sec.StreamPosition.exact(linkRevision)
      val let  = sec.EventType.LinkTo
      val led  = sec.EventData(let, JUUID.fromString(linkId), linkData, linkCustomMeta, sec.ContentType.Binary)

      //

      val streamEventRecord =
        EventRecord(sid, sp, ed, created)

      val linkStreamEventRecord =
        sec.EventRecord(lsid, lsp, led, linkCreated)

      test(streamEventRecord, linkStreamEventRecord, mkStreamPosition[ErrorOr])

      val allEventRecord =
        EventRecord(sid, sec.PositionInfo.Global(sp, sec.LogPosition.exact(commit, prepare)), ed, created)

      val linkAllEventRecord =
        sec.EventRecord(lsid,
                        sec.PositionInfo.Global(lsp, sec.LogPosition.exact(linkCommit, linkPrepare)),
                        led,
                        linkCreated)

      test(allEventRecord, linkAllEventRecord, mkPositionGlobal[ErrorOr])

    }

    "mkEventRecord" >> {

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
        mkEventRecord[ErrorOr, P](recordedEvent, mkPos) shouldEqual eventRecord.asRight

        // Bad StreamId
        mkEventRecord[ErrorOr, P](recordedEvent.withStreamIdentifier("".toStreamIdentifer), mkPos) shouldEqual
          ProtoResultError("name cannot be empty").asLeft

        // Missing UUID
        mkEventRecord[ErrorOr, P](recordedEvent.withId(UUID().withValue(UUID.Value.Empty)), mkPos) shouldEqual
          ProtoResultError("UUID is missing").asLeft

        // Bad UUID
        mkEventRecord[ErrorOr, P](recordedEvent.withId(UUID().withString("invalid")), mkPos) shouldEqual
          ProtoResultError("Invalid UUID string: invalid").asLeft

        // Missing EventType
        mkEventRecord[ErrorOr, P](
          recordedEvent.withMetadata(metadata.view.filterKeys(_ != Type).toMap),
          mkPos
        ) shouldEqual ProtoResultError(s"Required value $Type missing or invalid.").asLeft

        // Missing ContentType
        mkEventRecord[ErrorOr, P](
          recordedEvent.withMetadata(metadata.view.filterKeys(_ != ContentType).toMap),
          mkPos
        ) shouldEqual ProtoResultError(s"Required value $ContentType missing or invalid.").asLeft

        // Bad ContentType
        mkEventRecord[ErrorOr, P](recordedEvent.withMetadata(metadata.updated(ContentType, "no")), mkPos) shouldEqual
          ProtoResultError(s"Required value $ContentType missing or invalid: no").asLeft

        // Missing Created
        mkEventRecord[ErrorOr, P](
          recordedEvent.withMetadata(metadata.view.filterKeys(_ != Created).toMap),
          mkPos
        ) shouldEqual ProtoResultError(s"Required value $Created missing or invalid.").asLeft

        // Bad Created
        mkEventRecord[ErrorOr, P](
          recordedEvent.withMetadata(metadata.updated(Created, "chuck norris")),
          mkPos
        ) shouldEqual ProtoResultError(s"Required value $Created missing or invalid.").asLeft
      }

      ///

      val sid = sec.StreamId(streamId).unsafe
      val et  = sec.EventType(eventType).unsafe
      val sp  = sec.StreamPosition.exact(revision)
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

    "mkCheckpoint" >> {

      mkCheckpoint[ErrorOr](s.ReadResp.Checkpoint(1L, 1L)) shouldEqual
        Checkpoint(sec.LogPosition.exact(1L, 1L)).asRight

      mkCheckpoint[ErrorOr](s.ReadResp.Checkpoint(-1L, 1L)) shouldEqual
        ProtoResultError("Invalid position for Checkpoint: commit must be >= 0, but is -1").asLeft
    }

    "mkCheckpointOrEvent" >> {

      import arbitraries._

      val created     = Instant.EPOCH.atZone(ZoneOffset.UTC)
      val event       = sampleOfGen(eventGen.allEventRecordOne).copy(created = created)
      val eventData   = event.eventData
      val eventType   = sec.EventType.eventTypeToString(event.eventData.eventType)
      val contentType = eventData.contentType.fold(Binary, Json)
      val metadata    = Map(ContentType -> contentType, Type -> eventType, Created -> created.getNano().toString)

      val recordedEvent = s.ReadResp.ReadEvent
        .RecordedEvent()
        .withStreamIdentifier(event.streamId.stringValue.toStreamIdentifer)
        .withStreamRevision(event.streamPosition.value)
        .withCommitPosition(event.logPosition.commit)
        .withPreparePosition(event.logPosition.prepare)
        .withData(event.eventData.data.toByteString)
        .withCustomMetadata(event.eventData.metadata.toByteString)
        .withId(UUID().withString(event.eventData.eventId.toString))
        .withMetadata(metadata)

      val checkpoint = s.ReadResp.Checkpoint(1L, 1L)

      mkCheckpointOrEvent[ErrorOr](s.ReadResp().withEvent(s.ReadResp.ReadEvent().withEvent(recordedEvent))) shouldEqual
        Some(event.asRight[Checkpoint]).asRight

      mkCheckpointOrEvent[ErrorOr](s.ReadResp().withCheckpoint(checkpoint)) shouldEqual
        Some(Checkpoint(sec.LogPosition.exact(1L, 1L)).asLeft[AllEvent]).asRight

      mkCheckpointOrEvent[ErrorOr](s.ReadResp()) shouldEqual
        None.asRight[Either[Checkpoint, AllEvent]]
    }

    "mkStreamNotFound" >> {

      val sn  = s.ReadResp.StreamNotFound()
      val sni = sn.withStreamIdentifier("abc".toStreamIdentifer)

      mkStreamNotFound[ErrorOr](sn) shouldEqual
        ProtoResultError("Required value StreamIdentifer missing or invalid.").asLeft

      mkStreamNotFound[ErrorOr](sni) shouldEqual StreamNotFound("abc").asRight

    }

    "failStreamNotFound" >> {

      val rr = s.ReadResp()
      val cp = rr.withCheckpoint(s.ReadResp.Checkpoint(1L, 1L))
      val sn = rr.withStreamNotFound(s.ReadResp.StreamNotFound().withStreamIdentifier("abc".toStreamIdentifer))

      failStreamNotFound[ErrorOr](sn) shouldEqual StreamNotFound("abc").asLeft
      failStreamNotFound[ErrorOr](cp) shouldEqual cp.asRight

    }

    "reqConfirmation" >> {

      reqConfirmation[ErrorOr](s.ReadResp()) shouldEqual
        ProtoResultError("Required value SubscriptionConfirmation missing or invalid. Got Empty instead").asLeft

      reqConfirmation[ErrorOr](s.ReadResp().withConfirmation(s.ReadResp.SubscriptionConfirmation("id"))) shouldEqual
        SubscriptionConfirmation("id").asRight

    }

    "mkEventType" >> {
      mkEventType[ErrorOr](null) shouldEqual ProtoResultError("Event type name cannot be empty").asLeft
      mkEventType[ErrorOr]("") shouldEqual ProtoResultError("Event type name cannot be empty").asLeft
      mkEventType[ErrorOr]("sec.protos.A") shouldEqual sec.EventType.normal("sec.protos.A").unsafe.asRight
      mkEventType[ErrorOr]("$system-type") shouldEqual sec.EventType.system("system-type").unsafe.asRight
      mkEventType[ErrorOr]("$>") shouldEqual sec.EventType.LinkTo.asRight
    }

    "mkWriteResult" >> {

      import s.AppendResp.{Result, Success}
      import s.AppendResp.WrongExpectedVersion.ExpectedRevisionOption
      import s.AppendResp.WrongExpectedVersion.CurrentRevisionOption

      val sid: StreamId.Id                           = sec.StreamId("abc").unsafe
      val test: s.AppendResp => ErrorOr[WriteResult] = mkWriteResult[ErrorOr](sid, _)

      val successRevOne   = Success().withCurrentRevision(1L).withPosition(s.AppendResp.Position(1L, 1L))
      val successRevEmpty = Success().withCurrentRevisionOption(Success.CurrentRevisionOption.Empty)
      val successNoStream = Success().withNoStream(empty)

      test(s.AppendResp().withSuccess(successRevOne)) shouldEqual
        WriteResult(sec.StreamPosition.exact(1L), sec.LogPosition.exact(1L, 1L)).asRight

      test(s.AppendResp().withSuccess(successNoStream)) shouldEqual
        ProtoResultError("Did not expect NoStream when using NonEmptyList").asLeft

      test(s.AppendResp().withSuccess(successRevEmpty)) shouldEqual
        ProtoResultError("CurrentRevisionOption is missing").asLeft

      test(s.AppendResp().withSuccess(successRevOne.withNoPosition(empty))) shouldEqual
        ProtoResultError("Did not expect NoPosition when using NonEmptyList").asLeft

      test(s.AppendResp().withSuccess(successRevOne.withPositionOption(Success.PositionOption.Empty))) shouldEqual
        ProtoResultError("PositionOption is missing").asLeft

      ///

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
        test(mkExpected(ero)) shouldEqual WrongExpectedState(sid, expected, sec.StreamPosition.exact(2L)).asLeft

      testExpected(wreExpectedOne, sec.StreamPosition.exact(1L))
      testExpected(wreExpectedNoStream, sec.StreamState.NoStream)
      testExpected(wreExpectedAny, sec.StreamState.Any)
      testExpected(wreExpectedStreamExists, sec.StreamState.StreamExists)
      test(mkExpected(wreExpectedEmpty)) shouldEqual ProtoResultError("ExpectedRevisionOption is missing").asLeft

      def mkCurrent(c: CurrentRevisionOption) = s
        .AppendResp()
        .withWrongExpectedVersion(
          s.AppendResp.WrongExpectedVersion(expectedRevisionOption = wreExpectedOne, currentRevisionOption = c)
        )

      def testCurrent(cro: CurrentRevisionOption, actual: StreamState) =
        test(mkCurrent(cro)) shouldEqual WrongExpectedState(sid, sec.StreamPosition.exact(1L), actual).asLeft

      testCurrent(wreCurrentRevTwo, sec.StreamPosition.exact(2L))
      testCurrent(wreCurrentNoStream, sec.StreamState.NoStream)
      test(mkCurrent(wreCurrentEmpty)) shouldEqual ProtoResultError("CurrentRevisionOption is missing").asLeft

      ///

      test(s.AppendResp().withResult(Result.Empty)) shouldEqual
        ProtoResultError("Result is missing").asLeft

    }

    "mkDeleteResult" >> {
      mkDeleteResult[ErrorOr](s.DeleteResp().withPosition(s.DeleteResp.Position(1L, 1L))) shouldEqual
        DeleteResult(sec.LogPosition.exact(1L, 1L)).asRight

      mkDeleteResult[ErrorOr](s.DeleteResp().withNoPosition(empty)) shouldEqual
        ProtoResultError("Required value DeleteResp.PositionOptions.Position missing or invalid.").asLeft
    }

    "mkTombstoneResult" >> {
      mkTombstoneResult[ErrorOr](s.TombstoneResp().withPosition(s.TombstoneResp.Position(1L, 1L))) shouldEqual
        TombstoneResult(sec.LogPosition.exact(1L, 1L)).asRight

      mkTombstoneResult[ErrorOr](s.TombstoneResp().withNoPosition(empty)) shouldEqual
        ProtoResultError("Required value TombstoneResp.PositionOptions.Position missing or invalid.").asLeft
    }

  }
}
