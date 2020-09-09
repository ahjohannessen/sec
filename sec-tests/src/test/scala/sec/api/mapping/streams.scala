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
import com.eventstore.client._
import com.eventstore.client.streams._
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

    val empty = com.eventstore.client.Empty()

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

      mapReadEventFilter(None) shouldEqual NoFilter(empty)

      mapReadEventFilter(prefix(ByStreamId, 10.some, "a", "b").some) shouldEqual
        Filter(FilterOptions().withStreamName(Expression().withPrefix(List("a", "b"))).withMax(10))

      mapReadEventFilter(prefix(ByStreamId, None, "a").some) shouldEqual
        Filter(FilterOptions().withStreamName(Expression().withPrefix(List("a"))).withCount(empty))

      mapReadEventFilter(prefix(ByEventType, 10.some, "a", "b").some) shouldEqual
        Filter(FilterOptions().withEventType(Expression().withPrefix(List("a", "b"))).withMax(10))

      mapReadEventFilter(prefix(ByEventType, None, "a").some) shouldEqual
        Filter(FilterOptions().withEventType(Expression().withPrefix(List("a"))).withCount(empty))

      mapReadEventFilter(regex(ByStreamId, 10.some, "^[^$].*").some) shouldEqual
        Filter(FilterOptions().withStreamName(Expression().withRegex("^[^$].*")).withMax(10))

      mapReadEventFilter(regex(ByStreamId, None, "^(ns_).+").some) shouldEqual
        Filter(FilterOptions().withStreamName(Expression().withRegex("^(ns_).+")).withCount(empty))

      mapReadEventFilter(regex(ByEventType, 10.some, "^[^$].*").some) shouldEqual
        Filter(FilterOptions().withEventType(Expression().withRegex("^[^$].*")).withMax(10))

      mapReadEventFilter(regex(ByEventType, None, "^(ns_).+").some) shouldEqual
        Filter(FilterOptions().withEventType(Expression().withRegex("^(ns_).+")).withCount(empty))
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

      def test(exclusiveFrom: Option[c.Position], resolveLinkTos: Boolean, filter: Option[EventFilter]) =
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
        fi <- List(Option.empty[EventFilter], prefix(ByStreamId, None, "abc").some)
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

    "mkSoftDeleteReq" >> {

      import DeleteReq.Options.ExpectedStreamRevision
      val sid = c.StreamId.from("abc").unsafe

      def test(sr: c.StreamRevision, esr: ExpectedStreamRevision) =
        mkSoftDeleteReq(sid, sr) shouldEqual
          DeleteReq().withOptions(DeleteReq.Options(sid.esSid.some, esr))

      test(c.EventNumber.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(c.StreamRevision.NoStream, ExpectedStreamRevision.NoStream(empty))
      test(c.StreamRevision.StreamExists, ExpectedStreamRevision.StreamExists(empty))
      test(c.StreamRevision.Any, ExpectedStreamRevision.Any(empty))
    }

    "mkHardDeleteReq" >> {

      import TombstoneReq.Options.ExpectedStreamRevision
      val sid = c.StreamId.from("abc").unsafe

      def test(sr: c.StreamRevision, esr: ExpectedStreamRevision) =
        mkHardDeleteReq(sid, sr) shouldEqual
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
    import Streams.{DeleteResult, WriteResult}
    import grpc.constants.Metadata.{ContentType, ContentTypes, Created, Type}
    import ContentTypes.{ApplicationJson => Json, ApplicationOctetStream => Binary}

    val empty = com.eventstore.client.Empty()

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
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.updated(ContentType, "no").toMap)) shouldEqual
        ProtoResultError(s"Required value $ContentType missing or invalid: no").asLeft

      // Missing Created
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.view.filterKeys(_ != Created).toMap)) shouldEqual
        ProtoResultError(s"Required value $Created missing or invalid.").asLeft

      // Bad Created
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.updated(Created, "chuck norris").toMap)) shouldEqual
        ProtoResultError(s"Required value $Created missing or invalid.").asLeft
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

      val test: AppendResp => ErrorOr[WriteResult] = mkWriteResult[ErrorOr](c.StreamId.from("abc").unsafe, _)

      test(AppendResp().withSuccess(Success().withCurrentRevision(1L))) shouldEqual
        WriteResult(c.EventNumber.exact(1L)).asRight

      test(AppendResp().withSuccess(Success().withNoStream(empty))) shouldEqual
        ProtoResultError("Did not expect NoStream when using NonEmptyList").asLeft

      test(
        AppendResp().withSuccess(Success().withCurrentRevisionOption(Success.CurrentRevisionOption.Empty))) shouldEqual
        ProtoResultError("CurrentRevisionOptions is missing").asLeft

      // TODO: WrongExpectedVersion

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
