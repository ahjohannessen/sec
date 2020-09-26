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
import com.eventstore.dbclient.proto.shared._
import com.eventstore.dbclient.proto.streams._
import sec.api.exceptions.{StreamNotFound, WrongExpectedRevision}
import sec.{core => c}
import sec.api.mapping.shared._
import sec.api.mapping.streams.outgoing
import sec.api.mapping.streams.incoming
import sec.api.mapping.implicits._

class StreamsMappingSpec extends mutable.Specification {

  "outgoing" >> {

    import outgoing._
    import ReadReq.Options.AllOptions.AllOption
    import ReadReq.Options.StreamOptions.RevisionOption

    val empty = Empty()

    ///

    "uuidOption" >> {
      uuidOption shouldEqual ReadReq.Options.UUIDOption().withStructured(empty)
    }

    "mapPosition" >> {
      mapPosition(c.Position.exact(1L, 2L)) shouldEqual AllOption.Position(ReadReq.Options.Position(1L, 2L))
      mapPosition(c.Position.End) shouldEqual AllOption.End(empty)
      mapPositionOpt(c.Position.exact(0L, 0L).some) shouldEqual AllOption.Position(ReadReq.Options.Position(0L, 0L))
      mapPositionOpt(None) shouldEqual AllOption.Start(empty)
    }

    "mapEventNumber" >> {
      mapEventNumber(c.EventNumber.exact(1L)) shouldEqual RevisionOption.Revision(1L)
      mapEventNumber(c.EventNumber.End) shouldEqual RevisionOption.End(empty)
      mapEventNumberOpt(c.EventNumber.exact(0L).some) shouldEqual RevisionOption.Revision(0L)
      mapEventNumberOpt(None) shouldEqual RevisionOption.Start(empty)
    }

    "mapDirection" >> {
      mapDirection(Direction.Forwards) shouldEqual ReadReq.Options.ReadDirection.Forwards
      mapDirection(Direction.Backwards) shouldEqual ReadReq.Options.ReadDirection.Backwards
    }

    "mapReadEventFilter" >> {

      import EventFilter._
      import ReadReq.Options.FilterOptions
      import ReadReq.Options.FilterOptions.Expression
      import ReadReq.Options.FilterOption.{Filter, NoFilter}

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
      import c.StreamId
      import c.EventNumber._

      val sid = StreamId.from("abc").unsafe

      def test(exclusiveFrom: Option[c.EventNumber], resolveLinkTos: Boolean) =
        mkSubscribeToStreamReq(sid, exclusiveFrom, resolveLinkTos) shouldEqual ReadReq().withOptions(
          ReadReq
            .Options()
            .withStream(ReadReq.Options.StreamOptions(sid.esSid.some, mapEventNumberOpt(exclusiveFrom)))
            .withSubscription(ReadReq.Options.SubscriptionOptions())
            .withReadDirection(ReadReq.Options.ReadDirection.Forwards)
            .withResolveLinks(resolveLinkTos)
            .withNoFilter(empty)
            .withUuidOption(uuidOption)
        )

      for {
        ef <- List(Option.empty[c.EventNumber], Start.some, exact(1337L).some, End.some)
        rt <- List(true, false)
      } yield test(ef, rt)
    }

    "mkSubscribeToAllReq" >> {

      import c.Position._
      import EventFilter._

      def test(exclusiveFrom: Option[c.Position], resolveLinkTos: Boolean, filter: Option[SubscriptionFilterOptions]) =
        mkSubscribeToAllReq(exclusiveFrom, resolveLinkTos, filter) shouldEqual ReadReq().withOptions(
          ReadReq
            .Options()
            .withAll(ReadReq.Options.AllOptions(mapPositionOpt(exclusiveFrom)))
            .withSubscription(ReadReq.Options.SubscriptionOptions())
            .withReadDirection(ReadReq.Options.ReadDirection.Forwards)
            .withResolveLinks(resolveLinkTos)
            .withFilterOption(mapReadEventFilter(filter))
            .withUuidOption(uuidOption)
        )

      for {
        ef <- List(Option.empty[c.Position], Start.some, exact(1337L, 1337L).some, End.some)
        rt <- List(true, false)
        fi <- List(
                Option.empty[SubscriptionFilterOptions],
                SubscriptionFilterOptions(streamIdPrefix("abc"), 64.some, 1).some,
                SubscriptionFilterOptions(eventTypeRegex("^[^$].*")).some
              )
      } yield test(ef, rt, fi)
    }

    "mkReadStreamReq" >> {

      val sid = c.StreamId.from("abc").unsafe

      def test(rd: Direction, from: c.EventNumber, count: Long, rlt: Boolean) =
        mkReadStreamReq(sid, from, rd, count, rlt) shouldEqual ReadReq().withOptions(
          ReadReq
            .Options()
            .withStream(ReadReq.Options.StreamOptions(sid.esSid.some, mapEventNumber(from)))
            .withCount(count)
            .withReadDirection(mapDirection(rd))
            .withResolveLinks(rlt)
            .withNoFilter(empty)
            .withUuidOption(uuidOption)
        )

      for {
        rd <- List(Direction.Forwards, Direction.Backwards)
        fr <- List(c.EventNumber.Start, c.EventNumber.exact(2200), c.EventNumber.End)
        ct <- List(100L, 1000L, 10000L)
        rt <- List(true, false)
      } yield test(rd, fr, ct, rt)
    }

    "mkReadAllReq" >> {

      def test(from: c.Position, rd: Direction, maxCount: Long, rlt: Boolean) =
        mkReadAllReq(from, rd, maxCount, rlt) shouldEqual ReadReq().withOptions(
          ReadReq
            .Options()
            .withAll(ReadReq.Options.AllOptions(mapPosition(from)))
            .withSubscription(ReadReq.Options.SubscriptionOptions())
            .withCount(maxCount)
            .withReadDirection(mapDirection(rd))
            .withResolveLinks(rlt)
            .withNoFilter(empty)
            .withUuidOption(uuidOption)
        )

      for {
        fr <- List(c.Position.Start, c.Position.exact(1337L, 1337L), c.Position.End)
        rd <- List(Direction.Forwards, Direction.Backwards)
        mc <- List(100L, 1000L, 10000L)
        rt <- List(true, false)
      } yield test(fr, rd, mc, rt)
    }

    "mkDeleteReq" >> {

      import DeleteReq.Options.ExpectedStreamRevision
      val sid = c.StreamId.from("abc").unsafe

      def test(sr: c.StreamRevision, esr: ExpectedStreamRevision) =
        mkDeleteReq(sid, sr) shouldEqual
          DeleteReq().withOptions(DeleteReq.Options(sid.esSid.some, esr))

      test(c.EventNumber.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(c.StreamRevision.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(c.StreamRevision.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(c.StreamRevision.Any, ExpectedStreamRevision.Any(empty))
    }

    "mkTombstoneReq" >> {

      import TombstoneReq.Options.ExpectedStreamRevision
      val sid = c.StreamId.from("abc").unsafe

      def test(sr: c.StreamRevision, esr: ExpectedStreamRevision) =
        mkTombstoneReq(sid, sr) shouldEqual
          TombstoneReq().withOptions(TombstoneReq.Options(sid.esSid.some, esr))

      test(c.EventNumber.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(c.StreamRevision.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(c.StreamRevision.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(c.StreamRevision.Any, ExpectedStreamRevision.Any(empty))
    }

    "mkAppendHeaderReq" >> {

      import AppendReq.Options.ExpectedStreamRevision
      val sid = c.StreamId.from("abc").unsafe

      def test(sr: c.StreamRevision, esr: ExpectedStreamRevision) =
        mkAppendHeaderReq(sid, sr) shouldEqual
          AppendReq().withOptions(AppendReq.Options(sid.esSid.some, esr))

      test(c.EventNumber.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(c.StreamRevision.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(c.StreamRevision.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(c.StreamRevision.Any, ExpectedStreamRevision.Any(empty))
    }

    "mkAppendProposalsReq" >> {

      import grpc.constants.Metadata.{ContentType, ContentTypes, Type}
      import ContentTypes.{ApplicationJson => Json, ApplicationOctetStream => Binary}

      def json(nr: Int): c.EventData = {
        val id = JUUID.randomUUID()
        val et = s"et-$nr"
        val da = c.Content.json(s"""{ "data" : "$nr" }""").unsafe
        val md = c.Content.json(s"""{ "meta" : "$nr" }""").unsafe
        c.EventData(et, id, da, md).unsafe
      }

      def binary(nr: Int): c.EventData = {
        val id = JUUID.randomUUID()
        val et = s"et-$nr"
        val da = c.Content.binary(s"data@$nr").unsafe
        val md = c.Content.binary(s"meta@$nr").unsafe
        c.EventData(et, id, da, md).unsafe
      }

      def test(nel: NonEmptyList[c.EventData]) = {
        mkAppendProposalsReq(nel) shouldEqual nel.zipWithIndex.map { case (e, i) =>
          AppendReq().withProposedMessage(
            AppendReq
              .ProposedMessage()
              .withId(mkUuid(e.eventId))
              .withMetadata(Map(Type -> s"et-$i", ContentType -> e.contentType.fold(Binary, Json)))
              .withData(e.data.bytes.toByteString)
              .withCustomMetadata(e.metadata.bytes.toByteString)
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
    import Streams.{Checkpoint, DeleteResult, WriteResult}
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
        ReadResp.ReadEvent
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
      val linkEventType  = c.EventType.systemTypes.LinkTo
      val linkData       = c.Content.binary(s"$revision@$streamId").unsafe.bytes
      val linkCustomMeta = ByteVector.empty
      val linkCreated    = Instant.EPOCH.atZone(ZoneOffset.UTC)
      val linkMetadata   = Map(ContentType -> Binary, Type -> linkEventType, Created -> linkCreated.getNano().toString)

      val linkProto = ReadResp.ReadEvent
        .RecordedEvent()
        .withStreamIdentifier(linkStreamId.toStreamIdentifer)
        .withStreamRevision(linkRevision)
        .withCommitPosition(linkCommit)
        .withPreparePosition(linkPrepare)
        .withData(linkData.toByteString)
        .withCustomMetadata(linkCustomMeta.toByteString)
        .withId(UUID().withString(linkId))
        .withMetadata(linkMetadata)

      val eventRecord = c.EventRecord(
        c.StreamId.from(streamId).unsafe,
        c.EventNumber.exact(revision),
        c.Position.exact(commit, prepare),
        c.EventData.json(c.EventType(eventType).unsafe, JUUID.fromString(id), data, customMeta),
        created
      )

      val linkRecord = c.EventRecord(
        c.StreamId.from(linkStreamId).unsafe,
        c.EventNumber.exact(linkRevision),
        c.Position.exact(linkCommit, linkPrepare),
        c.EventData.binary(c.EventType.LinkTo, JUUID.fromString(linkId), linkData, linkCustomMeta),
        linkCreated
      )

      ///

      val readEvent = ReadResp.ReadEvent()

      // Event & No Link => EventRecord
      mkEvent[ErrorOr](readEvent.withEvent(eventProto)) shouldEqual eventRecord.some.asRight

      // Event & Link => ResolvedEvent
      mkEvent[ErrorOr](readEvent.withEvent(eventProto).withLink(linkProto)) shouldEqual
        c.ResolvedEvent(eventRecord, linkRecord).some.asRight

      // No Event & No Link => None
      mkEvent[ErrorOr](readEvent) shouldEqual Option.empty[c.Event].asRight

      // No Event & Link, i.e. link to deleted event => None
      mkEvent[ErrorOr](readEvent.withLink(linkProto)) shouldEqual Option.empty[c.Event].asRight
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
        ReadResp.ReadEvent
          .RecordedEvent()
          .withStreamIdentifier(streamId.toStreamIdentifer)
          .withStreamRevision(revision)
          .withCommitPosition(commit)
          .withPreparePosition(prepare)
          .withData(data.toByteString)
          .withCustomMetadata(customMeta.toByteString)
          .withId(UUID().withString(id))
          .withMetadata(metadata)

      val eventRecord = c.EventRecord(
        c.StreamId.from(streamId).unsafe,
        c.EventNumber.exact(revision),
        c.Position.exact(commit, prepare),
        c.EventData.binary(c.EventType(eventType).unsafe, JUUID.fromString(id), data, customMeta),
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

      mkCheckpoint[ErrorOr](ReadResp.Checkpoint(1L, 1L)) shouldEqual
        Checkpoint(c.Position.exact(1L, 1L)).asRight

      mkCheckpoint[ErrorOr](ReadResp.Checkpoint(-1L, 1L)) shouldEqual
        ProtoResultError("Invalid position for Checkpoint: commit must be >= 0, but is -1").asLeft
    }

    "mkCheckpointOrEvent" >> {

      import arbitraries._

      val created     = Instant.EPOCH.atZone(ZoneOffset.UTC)
      val event       = sampleOfGen(eventGen.eventRecordOne).copy(created = created)
      val eventData   = event.eventData
      val eventType   = c.EventType.eventTypeToString(event.eventData.eventType)
      val contentType = eventData.data.contentType.isBinary.fold(Binary, Json)
      val metadata    = Map(ContentType -> contentType, Type -> eventType, Created -> created.getNano().toString)

      val recordedEvent = ReadResp.ReadEvent
        .RecordedEvent()
        .withStreamIdentifier(event.streamId.stringValue.toStreamIdentifer)
        .withStreamRevision(event.number.value)
        .withCommitPosition(event.position.commit)
        .withPreparePosition(event.position.prepare)
        .withData(event.eventData.data.bytes.toByteString)
        .withCustomMetadata(event.eventData.metadata.bytes.toByteString)
        .withId(UUID().withString(event.eventData.eventId.toString))
        .withMetadata(metadata)

      val checkpoint = ReadResp.Checkpoint(1L, 1L)

      mkCheckpointOrEvent[ErrorOr](ReadResp().withEvent(ReadResp.ReadEvent().withEvent(recordedEvent))) shouldEqual
        Some(event.asRight[Checkpoint]).asRight

      mkCheckpointOrEvent[ErrorOr](ReadResp().withCheckpoint(checkpoint)) shouldEqual
        Some(Checkpoint(c.Position.exact(1L, 1L)).asLeft[c.Event]).asRight

      mkCheckpointOrEvent[ErrorOr](ReadResp()) shouldEqual
        None.asRight[Either[Checkpoint, c.Event]]
    }

    "mkStreamNotFound" >> {

      mkStreamNotFound[ErrorOr](ReadResp.StreamNotFound()) shouldEqual
        ProtoResultError("Required value StreamIdentifer expected missing or invalid.").asLeft

      mkStreamNotFound[ErrorOr](ReadResp.StreamNotFound().withStreamIdentifier("abc".toStreamIdentifer)) shouldEqual
        StreamNotFound("abc").asRight

    }

    "mkEventType" >> {
      mkEventType[ErrorOr](null) shouldEqual ProtoResultError("Event type name cannot be empty").asLeft
      mkEventType[ErrorOr]("") shouldEqual ProtoResultError("Event type name cannot be empty").asLeft
      mkEventType[ErrorOr]("sec.protos.A") shouldEqual c.EventType.userDefined("sec.protos.A").unsafe.asRight
      mkEventType[ErrorOr]("$system-type") shouldEqual c.EventType.systemDefined("system-type").unsafe.asRight
      mkEventType[ErrorOr]("$>") shouldEqual c.EventType.LinkTo.asRight
    }

    "mkWriteResult" >> {

      import AppendResp.{Result, Success}
      import AppendResp.WrongExpectedVersion.ExpectedRevisionOption
      import AppendResp.WrongExpectedVersion.CurrentRevisionOption

      val sid: c.StreamId.Id                       = c.StreamId.from("abc").unsafe
      val test: AppendResp => ErrorOr[WriteResult] = mkWriteResult[ErrorOr](sid, _)

      val successRevOne   = Success().withCurrentRevision(1L).withPosition(AppendResp.Position(1L, 1L))
      val successRevEmpty = Success().withCurrentRevisionOption(Success.CurrentRevisionOption.Empty)
      val successNoStream = Success().withNoStream(empty)

      test(AppendResp().withSuccess(successRevOne)) shouldEqual
        WriteResult(c.EventNumber.exact(1L), c.Position.exact(1L, 1L)).asRight

      test(AppendResp().withSuccess(successNoStream)) shouldEqual
        ProtoResultError("Did not expect NoStream when using NonEmptyList").asLeft

      test(AppendResp().withSuccess(successRevEmpty)) shouldEqual
        ProtoResultError("CurrentRevisionOption is missing").asLeft

      test(AppendResp().withSuccess(successRevOne.withNoPosition(empty))) shouldEqual
        ProtoResultError("Did not expect NoPosition when using NonEmptyList").asLeft

      test(AppendResp().withSuccess(successRevOne.withPositionOption(Success.PositionOption.Empty))) shouldEqual
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

      def mkExpected(e: ExpectedRevisionOption) = AppendResp().withWrongExpectedVersion(
        AppendResp.WrongExpectedVersion(expectedRevisionOption = e, currentRevisionOption = wreCurrentRevTwo)
      )

      def testExpected(ero: ExpectedRevisionOption, expected: c.StreamRevision) =
        test(mkExpected(ero)) shouldEqual WrongExpectedRevision(sid, expected, c.EventNumber.exact(2L)).asLeft

      testExpected(wreExpectedOne, c.EventNumber.exact(1L))
      testExpected(wreExpectedNoStream, c.StreamRevision.NoStream)
      testExpected(wreExpectedAny, c.StreamRevision.Any)
      testExpected(wreExpectedStreamExists, c.StreamRevision.StreamExists)
      test(mkExpected(wreExpectedEmpty)) shouldEqual ProtoResultError("ExpectedRevisionOption is missing").asLeft

      def mkCurrent(c: CurrentRevisionOption) = AppendResp().withWrongExpectedVersion(
        AppendResp.WrongExpectedVersion(expectedRevisionOption = wreExpectedOne, currentRevisionOption = c)
      )

      def testCurrent(cro: CurrentRevisionOption, actual: c.StreamRevision) =
        test(mkCurrent(cro)) shouldEqual WrongExpectedRevision(sid, c.EventNumber.exact(1L), actual).asLeft

      testCurrent(wreCurrentRevTwo, c.EventNumber.exact(2L))
      testCurrent(wreCurrentNoStream, c.StreamRevision.NoStream)
      test(mkCurrent(wreCurrentEmpty)) shouldEqual ProtoResultError("CurrentRevisionOption is missing").asLeft

      ///

      test(AppendResp().withResult(Result.Empty)) shouldEqual
        ProtoResultError("Result is missing").asLeft

    }

    "mkDeleteResult" >> {
      mkDeleteResult[ErrorOr](DeleteResp().withPosition(DeleteResp.Position(1L, 1L))) shouldEqual
        DeleteResult(c.Position.exact(1L, 1L)).asRight

      mkDeleteResult[ErrorOr](DeleteResp().withNoPosition(empty)) shouldEqual
        ProtoResultError("Required value DeleteResp.PositionOptions.Position missing or invalid.").asLeft

      mkDeleteResult[ErrorOr](TombstoneResp().withPosition(TombstoneResp.Position(1L, 1L))) shouldEqual
        DeleteResult(c.Position.exact(1L, 1L)).asRight

      mkDeleteResult[ErrorOr](TombstoneResp().withNoPosition(empty)) shouldEqual
        ProtoResultError("Required value TombstoneResp.PositionOptions.Position missing or invalid.").asLeft
    }

  }
}
