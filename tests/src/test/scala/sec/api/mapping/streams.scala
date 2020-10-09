/*
 * Copyright 2020 Alex Henning Johannessen
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
import cats.syntax.all._
import cats.data.NonEmptyList
import scodec.bits.ByteVector
import org.specs2._
import com.eventstore.dbclient.proto.shared.{Empty, UUID}
import com.eventstore.dbclient.proto.{streams => s}
import sec.api.exceptions.{StreamNotFound, WrongExpectedRevision}
import sec.api.mapping.shared._
import sec.api.mapping.streams.outgoing
import sec.api.mapping.streams.incoming
import sec.api.mapping.implicits._
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

    "mapPosition" >> {
      mapPosition(sec.Position.exact(1L, 2L)) shouldEqual AllOption.Position(s.ReadReq.Options.Position(1L, 2L))
      mapPosition(sec.Position.End) shouldEqual AllOption.End(empty)
      mapPositionOpt(sec.Position.exact(0L, 0L).some) shouldEqual AllOption.Position(s.ReadReq.Options.Position(0L, 0L))
      mapPositionOpt(None) shouldEqual AllOption.Start(empty)
    }

    "mapEventNumber" >> {
      mapEventNumber(sec.EventNumber.exact(1L)) shouldEqual RevisionOption.Revision(1L)
      mapEventNumber(sec.EventNumber.End) shouldEqual RevisionOption.End(empty)
      mapEventNumberOpt(sec.EventNumber.exact(0L).some) shouldEqual RevisionOption.Revision(0L)
      mapEventNumberOpt(None) shouldEqual RevisionOption.Start(empty)
    }

    "mapDirection" >> {
      mapDirection(Direction.Forwards) shouldEqual s.ReadReq.Options.ReadDirection.Forwards
      mapDirection(Direction.Backwards) shouldEqual s.ReadReq.Options.ReadDirection.Backwards
    }

    "mapReadEventFilter" >> {

      import EventFilter._
      import s.ReadReq.Options.FilterOptions
      import s.ReadReq.Options.FilterOptions.Expression
      import s.ReadReq.Options.FilterOption.{Filter, NoFilter}

      def mkOptions(f: EventFilter, maxWindow: Option[Int] = 10.some, multiplier: Int = 1) =
        SubscriptionFilterOptions(f, maxWindow, multiplier).some

      mapReadEventFilter(None) shouldEqual NoFilter(empty)

      mapReadEventFilter(mkOptions(streamIdPrefix("a", "b"))) shouldEqual Filter(
        FilterOptions()
          .withStreamIdentifier(Expression().withPrefix(List("a", "b")))
          .withMax(10)
          .withCheckpointIntervalMultiplier(1)
      )

      mapReadEventFilter(mkOptions(streamIdPrefix("a"), None, 1)) shouldEqual Filter(
        FilterOptions()
          .withStreamIdentifier(Expression().withPrefix(List("a")))
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
          .withStreamIdentifier(Expression().withRegex("^[^$].*"))
          .withMax(10)
          .withCheckpointIntervalMultiplier(1)
      )

      mapReadEventFilter(mkOptions(streamIdRegex("^(ns_).+"), None, 1)) shouldEqual Filter(
        FilterOptions()
          .withStreamIdentifier(Expression().withRegex("^(ns_).+"))
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
      import EventNumber._

      val sid = StreamId.from("abc").unsafe

      def test(exclusiveFrom: Option[EventNumber], resolveLinkTos: Boolean) =
        mkSubscribeToStreamReq(sid, exclusiveFrom, resolveLinkTos) shouldEqual s
          .ReadReq()
          .withOptions(
            s.ReadReq
              .Options()
              .withStream(s.ReadReq.Options.StreamOptions(sid.esSid.some, mapEventNumberOpt(exclusiveFrom)))
              .withSubscription(s.ReadReq.Options.SubscriptionOptions())
              .withReadDirection(s.ReadReq.Options.ReadDirection.Forwards)
              .withResolveLinks(resolveLinkTos)
              .withNoFilter(empty)
              .withUuidOption(uuidOption)
          )

      for {
        ef <- List(Option.empty[EventNumber], Start.some, exact(1337L).some, End.some)
        rt <- List(true, false)
      } yield test(ef, rt)
    }

    "mkSubscribeToAllReq" >> {

      import Position._
      import EventFilter._

      def test(exclusiveFrom: Option[Position], resolveLinkTos: Boolean, filter: Option[SubscriptionFilterOptions]) =
        mkSubscribeToAllReq(exclusiveFrom, resolveLinkTos, filter) shouldEqual s
          .ReadReq()
          .withOptions(
            s.ReadReq
              .Options()
              .withAll(s.ReadReq.Options.AllOptions(mapPositionOpt(exclusiveFrom)))
              .withSubscription(s.ReadReq.Options.SubscriptionOptions())
              .withReadDirection(s.ReadReq.Options.ReadDirection.Forwards)
              .withResolveLinks(resolveLinkTos)
              .withFilterOption(mapReadEventFilter(filter))
              .withUuidOption(uuidOption)
          )

      for {
        ef <- List(Option.empty[Position], Start.some, exact(1337L, 1337L).some, End.some)
        rt <- List(true, false)
        fi <- List(
                Option.empty[SubscriptionFilterOptions],
                SubscriptionFilterOptions(streamIdPrefix("abc"), 64.some, 1).some,
                SubscriptionFilterOptions(eventTypeRegex("^[^$].*")).some
              )
      } yield test(ef, rt, fi)
    }

    "mkReadStreamReq" >> {

      val sid = sec.StreamId.from("abc").unsafe

      def test(rd: Direction, from: EventNumber, count: Long, rlt: Boolean) =
        mkReadStreamReq(sid, from, rd, count, rlt) shouldEqual s
          .ReadReq()
          .withOptions(
            s.ReadReq
              .Options()
              .withStream(s.ReadReq.Options.StreamOptions(sid.esSid.some, mapEventNumber(from)))
              .withCount(count)
              .withReadDirection(mapDirection(rd))
              .withResolveLinks(rlt)
              .withNoFilter(empty)
              .withUuidOption(uuidOption)
          )

      for {
        rd <- List(Direction.Forwards, Direction.Backwards)
        fr <- List(sec.EventNumber.Start, sec.EventNumber.exact(2200), sec.EventNumber.End)
        ct <- List(100L, 1000L, 10000L)
        rt <- List(true, false)
      } yield test(rd, fr, ct, rt)
    }

    "mkReadAllReq" >> {

      def test(from: Position, rd: Direction, maxCount: Long, rlt: Boolean) =
        mkReadAllReq(from, rd, maxCount, rlt) shouldEqual s
          .ReadReq()
          .withOptions(
            s.ReadReq
              .Options()
              .withAll(s.ReadReq.Options.AllOptions(mapPosition(from)))
              .withSubscription(s.ReadReq.Options.SubscriptionOptions())
              .withCount(maxCount)
              .withReadDirection(mapDirection(rd))
              .withResolveLinks(rlt)
              .withNoFilter(empty)
              .withUuidOption(uuidOption)
          )

      for {
        fr <- List(sec.Position.Start, sec.Position.exact(1337L, 1337L), sec.Position.End)
        rd <- List(Direction.Forwards, Direction.Backwards)
        mc <- List(100L, 1000L, 10000L)
        rt <- List(true, false)
      } yield test(fr, rd, mc, rt)
    }

    "mkDeleteReq" >> {

      import s.DeleteReq.Options.ExpectedStreamRevision
      val sid = sec.StreamId.from("abc").unsafe

      def test(sr: StreamRevision, esr: ExpectedStreamRevision) =
        mkDeleteReq(sid, sr) shouldEqual
          s.DeleteReq().withOptions(s.DeleteReq.Options(sid.esSid.some, esr))

      test(sec.EventNumber.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(sec.StreamRevision.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(sec.StreamRevision.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(sec.StreamRevision.Any, ExpectedStreamRevision.Any(empty))
    }

    "mkTombstoneReq" >> {

      import s.TombstoneReq.Options.ExpectedStreamRevision
      val sid = sec.StreamId.from("abc").unsafe

      def test(sr: StreamRevision, esr: ExpectedStreamRevision) =
        mkTombstoneReq(sid, sr) shouldEqual
          s.TombstoneReq().withOptions(s.TombstoneReq.Options(sid.esSid.some, esr))

      test(sec.EventNumber.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(sec.StreamRevision.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(sec.StreamRevision.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(sec.StreamRevision.Any, ExpectedStreamRevision.Any(empty))
    }

    "mkAppendHeaderReq" >> {

      import s.AppendReq.Options.ExpectedStreamRevision
      val sid = sec.StreamId.from("abc").unsafe

      def test(sr: StreamRevision, esr: ExpectedStreamRevision) =
        mkAppendHeaderReq(sid, sr) shouldEqual
          s.AppendReq().withOptions(s.AppendReq.Options(sid.esSid.some, esr))

      test(sec.EventNumber.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(sec.StreamRevision.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(sec.StreamRevision.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(sec.StreamRevision.Any, ExpectedStreamRevision.Any(empty))
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
      val linkEventType  = EventType.systemTypes.LinkTo
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

      val eventRecord = EventRecord(
        sec.StreamId.from(streamId).unsafe,
        sec.EventNumber.exact(revision),
        sec.Position.exact(commit, prepare),
        sec.EventData(sec.EventType(eventType).unsafe, JUUID.fromString(id), data, customMeta, sec.ContentType.Json),
        created
      )

      val linkRecord = sec.EventRecord(
        sec.StreamId.from(linkStreamId).unsafe,
        sec.EventNumber.exact(linkRevision),
        sec.Position.exact(linkCommit, linkPrepare),
        sec.EventData(sec.EventType.LinkTo, JUUID.fromString(linkId), linkData, linkCustomMeta, sec.ContentType.Binary),
        linkCreated
      )

      ///

      val readEvent = s.ReadResp.ReadEvent()

      // Event & No Link => EventRecord
      mkEvent[ErrorOr](readEvent.withEvent(eventProto)) shouldEqual eventRecord.some.asRight

      // Event & Link => ResolvedEvent
      mkEvent[ErrorOr](readEvent.withEvent(eventProto).withLink(linkProto)) shouldEqual
        ResolvedEvent(eventRecord, linkRecord).some.asRight

      // No Event & No Link => None
      mkEvent[ErrorOr](readEvent) shouldEqual Option.empty[Event].asRight

      // No Event & Link, i.e. link to deleted event => None
      mkEvent[ErrorOr](readEvent.withLink(linkProto)) shouldEqual Option.empty[Event].asRight

      // Require read event
      reqReadEvent[ErrorOr](s.ReadResp().withEvent(readEvent.withEvent(eventProto))) shouldEqual
        eventRecord.some.asRight

      reqReadEvent[ErrorOr](s.ReadResp()) shouldEqual
        ProtoResultError("Required value ReadEvent missing or invalid.").asLeft
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

      val recordedEvent =
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

      val eventRecord = sec.EventRecord(
        sec.StreamId.from(streamId).unsafe,
        sec.EventNumber.exact(revision),
        sec.Position.exact(commit, prepare),
        sec.EventData(sec.EventType(eventType).unsafe, JUUID.fromString(id), data, customMeta, sec.ContentType.Binary),
        created
      )

      // Happy Path
      mkEventRecord[ErrorOr](recordedEvent) shouldEqual eventRecord.asRight

      // Bad StreamId
      mkEventRecord[ErrorOr](recordedEvent.withStreamIdentifier("".toStreamIdentifer)) shouldEqual
        ProtoResultError("name cannot be empty").asLeft

      // Missing UUID
      mkEventRecord[ErrorOr](recordedEvent.withId(UUID().withValue(UUID.Value.Empty))) shouldEqual
        ProtoResultError("UUID is missing").asLeft

      // Bad UUID
      mkEventRecord[ErrorOr](recordedEvent.withId(UUID().withString("invalid"))) shouldEqual
        ProtoResultError("Invalid UUID string: invalid").asLeft

      // Missing EventType
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.view.filterKeys(_ != Type).toMap)) shouldEqual
        ProtoResultError(s"Required value $Type missing or invalid.").asLeft

      // Missing ContentType
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.view.filterKeys(_ != ContentType).toMap)) shouldEqual
        ProtoResultError(s"Required value $ContentType missing or invalid.").asLeft

      // Bad ContentType
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.updated(ContentType, "no"))) shouldEqual
        ProtoResultError(s"Required value $ContentType missing or invalid: no").asLeft

      // Missing Created
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.view.filterKeys(_ != Created).toMap)) shouldEqual
        ProtoResultError(s"Required value $Created missing or invalid.").asLeft

      // Bad Created
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.updated(Created, "chuck norris"))) shouldEqual
        ProtoResultError(s"Required value $Created missing or invalid.").asLeft
    }

    "mkCheckpoint" >> {

      mkCheckpoint[ErrorOr](s.ReadResp.Checkpoint(1L, 1L)) shouldEqual
        Checkpoint(sec.Position.exact(1L, 1L)).asRight

      mkCheckpoint[ErrorOr](s.ReadResp.Checkpoint(-1L, 1L)) shouldEqual
        ProtoResultError("Invalid position for Checkpoint: commit must be >= 0, but is -1").asLeft
    }

    "mkCheckpointOrEvent" >> {

      import arbitraries._

      val created     = Instant.EPOCH.atZone(ZoneOffset.UTC)
      val event       = sampleOfGen(eventGen.eventRecordOne).copy(created = created)
      val eventData   = event.eventData
      val eventType   = sec.EventType.eventTypeToString(event.eventData.eventType)
      val contentType = eventData.contentType.isBinary.fold(Binary, Json)
      val metadata    = Map(ContentType -> contentType, Type -> eventType, Created -> created.getNano().toString)

      val recordedEvent = s.ReadResp.ReadEvent
        .RecordedEvent()
        .withStreamIdentifier(event.streamId.stringValue.toStreamIdentifer)
        .withStreamRevision(event.number.value)
        .withCommitPosition(event.position.commit)
        .withPreparePosition(event.position.prepare)
        .withData(event.eventData.data.toByteString)
        .withCustomMetadata(event.eventData.metadata.toByteString)
        .withId(UUID().withString(event.eventData.eventId.toString))
        .withMetadata(metadata)

      val checkpoint = s.ReadResp.Checkpoint(1L, 1L)

      mkCheckpointOrEvent[ErrorOr](s.ReadResp().withEvent(s.ReadResp.ReadEvent().withEvent(recordedEvent))) shouldEqual
        Some(event.asRight[Checkpoint]).asRight

      mkCheckpointOrEvent[ErrorOr](s.ReadResp().withCheckpoint(checkpoint)) shouldEqual
        Some(Checkpoint(sec.Position.exact(1L, 1L)).asLeft[Event]).asRight

      mkCheckpointOrEvent[ErrorOr](s.ReadResp()) shouldEqual
        None.asRight[Either[Checkpoint, Event]]
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
        ProtoResultError("Required value SubscriptionConfirmation missing or invalid.").asLeft

      reqConfirmation[ErrorOr](s.ReadResp().withConfirmation(s.ReadResp.SubscriptionConfirmation("id"))) shouldEqual
        SubscriptionConfirmation("id").asRight

    }

    "mkEventType" >> {
      mkEventType[ErrorOr](null) shouldEqual ProtoResultError("Event type name cannot be empty").asLeft
      mkEventType[ErrorOr]("") shouldEqual ProtoResultError("Event type name cannot be empty").asLeft
      mkEventType[ErrorOr]("sec.protos.A") shouldEqual sec.EventType.userDefined("sec.protos.A").unsafe.asRight
      mkEventType[ErrorOr]("$system-type") shouldEqual sec.EventType.systemDefined("system-type").unsafe.asRight
      mkEventType[ErrorOr]("$>") shouldEqual sec.EventType.LinkTo.asRight
    }

    "mkWriteResult" >> {

      import s.AppendResp.{Result, Success}
      import s.AppendResp.WrongExpectedVersion.ExpectedRevisionOption
      import s.AppendResp.WrongExpectedVersion.CurrentRevisionOption

      val sid: StreamId.Id                           = sec.StreamId.from("abc").unsafe
      val test: s.AppendResp => ErrorOr[WriteResult] = mkWriteResult[ErrorOr](sid, _)

      val successRevOne   = Success().withCurrentRevision(1L).withPosition(s.AppendResp.Position(1L, 1L))
      val successRevEmpty = Success().withCurrentRevisionOption(Success.CurrentRevisionOption.Empty)
      val successNoStream = Success().withNoStream(empty)

      test(s.AppendResp().withSuccess(successRevOne)) shouldEqual
        WriteResult(sec.EventNumber.exact(1L), sec.Position.exact(1L, 1L)).asRight

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

      def testExpected(ero: ExpectedRevisionOption, expected: StreamRevision) =
        test(mkExpected(ero)) shouldEqual WrongExpectedRevision(sid, expected, sec.EventNumber.exact(2L)).asLeft

      testExpected(wreExpectedOne, sec.EventNumber.exact(1L))
      testExpected(wreExpectedNoStream, sec.StreamRevision.NoStream)
      testExpected(wreExpectedAny, sec.StreamRevision.Any)
      testExpected(wreExpectedStreamExists, sec.StreamRevision.StreamExists)
      test(mkExpected(wreExpectedEmpty)) shouldEqual ProtoResultError("ExpectedRevisionOption is missing").asLeft

      def mkCurrent(c: CurrentRevisionOption) = s
        .AppendResp()
        .withWrongExpectedVersion(
          s.AppendResp.WrongExpectedVersion(expectedRevisionOption = wreExpectedOne, currentRevisionOption = c)
        )

      def testCurrent(cro: CurrentRevisionOption, actual: StreamRevision) =
        test(mkCurrent(cro)) shouldEqual WrongExpectedRevision(sid, sec.EventNumber.exact(1L), actual).asLeft

      testCurrent(wreCurrentRevTwo, sec.EventNumber.exact(2L))
      testCurrent(wreCurrentNoStream, sec.StreamRevision.NoStream)
      test(mkCurrent(wreCurrentEmpty)) shouldEqual ProtoResultError("CurrentRevisionOption is missing").asLeft

      ///

      test(s.AppendResp().withResult(Result.Empty)) shouldEqual
        ProtoResultError("Result is missing").asLeft

    }

    "mkDeleteResult" >> {
      mkDeleteResult[ErrorOr](s.DeleteResp().withPosition(s.DeleteResp.Position(1L, 1L))) shouldEqual
        DeleteResult(sec.Position.exact(1L, 1L)).asRight

      mkDeleteResult[ErrorOr](s.DeleteResp().withNoPosition(empty)) shouldEqual
        ProtoResultError("Required value DeleteResp.PositionOptions.Position missing or invalid.").asLeft

      mkDeleteResult[ErrorOr](s.TombstoneResp().withPosition(s.TombstoneResp.Position(1L, 1L))) shouldEqual
        DeleteResult(sec.Position.exact(1L, 1L)).asRight

      mkDeleteResult[ErrorOr](s.TombstoneResp().withNoPosition(empty)) shouldEqual
        ProtoResultError("Required value TombstoneResp.PositionOptions.Position missing or invalid.").asLeft
    }

  }
}
