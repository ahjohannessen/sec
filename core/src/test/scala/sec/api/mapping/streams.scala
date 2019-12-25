package sec
package api
package mapping

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.{UUID => JUUID}
import cats.implicits._
import cats.data.NonEmptyList
import scodec.bits.ByteVector
import org.specs2._
import com.eventstore.client.streams._
import sec.{core => c}
import sec.api.mapping.streams.outgoing
import sec.api.mapping.streams.incoming
import sec.api.mapping.helpers._
class StreamsMappingSpec extends mutable.Specification {

  "outgoing" should {

    import outgoing._
    import ReadReq.Options.AllOptions.AllOptions
    import ReadReq.Options.StreamOptions.RevisionOptions

    val empty = ReadReq.Empty()

    ///

    "mapPosition" >> {
      mapPosition(c.Position.exact(1L, 2L)) shouldEqual AllOptions.Position(ReadReq.Options.Position(1L, 2L))
      mapPosition(c.Position.End) shouldEqual AllOptions.End(empty)
      mapPositionOpt(c.Position.exact(0L, 0L).some) shouldEqual AllOptions.Position(ReadReq.Options.Position(0L, 0L))
      mapPositionOpt(None) shouldEqual AllOptions.Start(empty)
    }

    "mapEventNumber" >> {
      mapEventNumber(c.EventNumber.exact(1L)) shouldEqual RevisionOptions.Revision(1L)
      mapEventNumber(c.EventNumber.End) shouldEqual RevisionOptions.End(empty)
      mapEventNumberOpt(c.EventNumber.exact(0L).some) shouldEqual RevisionOptions.Revision(0L)
      mapEventNumberOpt(None) shouldEqual RevisionOptions.Start(empty)
    }

    "mapDirection" >> {
      mapDirection(ReadDirection.Forward) shouldEqual ReadReq.Options.ReadDirection.Forwards
      mapDirection(ReadDirection.Backward) shouldEqual ReadReq.Options.ReadDirection.Backwards
    }

    "mapReadEventFilter" >> {

      import c.EventFilter._
      import ReadReq.Options.FilterOptions
      import ReadReq.Options.FilterOptions.Expression
      import ReadReq.Options.FilterOptionsOneof.{Filter, NoFilter}

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
            .withStream(ReadReq.Options.StreamOptions(sid.stringValue, mapEventNumberOpt(exclusiveFrom)))
            .withSubscription(ReadReq.Options.SubscriptionOptions())
            .withReadDirection(ReadReq.Options.ReadDirection.Forwards)
            .withResolveLinks(resolveLinkTos)
            .withNoFilter(empty)
        )

      for {
        ef <- List(Option.empty[c.EventNumber], Start.some, exact(1337L).some, End.some)
        rt <- List(true, false)
      } yield test(ef, rt)
    }

    "mkSubscribeToAllReq" >> {

      import c.Position._
      import c.EventFilter._

      def test(exclusiveFrom: Option[c.Position], resolveLinkTos: Boolean, filter: Option[c.EventFilter]) =
        mkSubscribeToAllReq(exclusiveFrom, resolveLinkTos, filter) shouldEqual ReadReq().withOptions(
          ReadReq
            .Options()
            .withAll(ReadReq.Options.AllOptions(mapPositionOpt(exclusiveFrom)))
            .withSubscription(ReadReq.Options.SubscriptionOptions())
            .withReadDirection(ReadReq.Options.ReadDirection.Forwards)
            .withResolveLinks(resolveLinkTos)
            .withFilterOptionsOneof(mapReadEventFilter(filter))
        )

      for {
        ef <- List(Option.empty[c.Position], Start.some, exact(1337L, 1337L).some, End.some)
        rt <- List(true, false)
        fi <- List(Option.empty[c.EventFilter], prefix(ByStreamId, None, "abc").some)
      } yield test(ef, rt, fi)
    }

    "mkReadStreamReq" >> {

      val sid = c.StreamId.from("abc").unsafe

      def test(rd: ReadDirection, from: c.EventNumber, count: Int, rlt: Boolean) =
        mkReadStreamReq(sid, from, rd, count, rlt) shouldEqual ReadReq().withOptions(
          ReadReq
            .Options()
            .withStream(ReadReq.Options.StreamOptions(sid.stringValue, mapEventNumber(from)))
            .withCount(count)
            .withReadDirection(mapDirection(rd))
            .withResolveLinks(rlt)
            .withNoFilter(empty)
        )

      for {
        rd <- List(ReadDirection.Forward, ReadDirection.Backward)
        fr <- List(c.EventNumber.Start, c.EventNumber.exact(2200), c.EventNumber.End)
        ct <- List(100, 1000, 10000)
        rt <- List(true, false)
      } yield test(rd, fr, ct, rt)
    }

    "mkReadAllReq" >> {

      import c.EventFilter._

      def test(from: c.Position, rd: ReadDirection, maxCount: Int, rlt: Boolean, filter: Option[c.EventFilter]) =
        mkReadAllReq(from, rd, maxCount, rlt, filter) shouldEqual ReadReq().withOptions(
          ReadReq
            .Options()
            .withAll(ReadReq.Options.AllOptions(mapPosition(from)))
            .withSubscription(ReadReq.Options.SubscriptionOptions())
            .withCount(maxCount)
            .withReadDirection(mapDirection(rd))
            .withResolveLinks(rlt)
            .withFilterOptionsOneof(mapReadEventFilter(filter))
        )

      for {
        fr <- List(c.Position.Start, c.Position.exact(1337L, 1337L), c.Position.End)
        rd <- List(ReadDirection.Forward, ReadDirection.Backward)
        mc <- List(100, 1000, 10000)
        rt <- List(true, false)
        fi <- List(Option.empty[c.EventFilter], prefix(ByStreamId, None, "abc").some)
      } yield test(fr, rd, mc, rt, fi)
    }

    "mkSoftDeleteReq" >> {

      import DeleteReq.Options.ExpectedStreamRevision
      val sid = c.StreamId.from("abc").unsafe

      def test(sr: c.StreamRevision, esr: ExpectedStreamRevision) =
        mkSoftDeleteReq(sid, sr) shouldEqual
          DeleteReq().withOptions(DeleteReq.Options(sid.stringValue, esr))

      test(c.EventNumber.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(c.StreamRevision.NoStream, ExpectedStreamRevision.NoStream(DeleteReq.Empty()))
      test(c.StreamRevision.StreamExists, ExpectedStreamRevision.StreamExists(DeleteReq.Empty()))
      test(c.StreamRevision.Any, ExpectedStreamRevision.Any(DeleteReq.Empty()))
    }

    "mkHardDeleteReq" >> {

      import TombstoneReq.Options.ExpectedStreamRevision
      val sid = c.StreamId.from("abc").unsafe

      def test(sr: c.StreamRevision, esr: ExpectedStreamRevision) =
        mkHardDeleteReq(sid, sr) shouldEqual
          TombstoneReq().withOptions(TombstoneReq.Options(sid.stringValue, esr))

      test(c.EventNumber.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(c.StreamRevision.NoStream, ExpectedStreamRevision.NoStream(TombstoneReq.Empty()))
      test(c.StreamRevision.StreamExists, ExpectedStreamRevision.StreamExists(TombstoneReq.Empty()))
      test(c.StreamRevision.Any, ExpectedStreamRevision.Any(TombstoneReq.Empty()))
    }

    "mkAppendHeaderReq" >> {

      import AppendReq.Options.ExpectedStreamRevision
      val sid = c.StreamId.from("abc").unsafe

      def test(sr: c.StreamRevision, esr: ExpectedStreamRevision) =
        mkAppendHeaderReq(sid, sr) shouldEqual
          AppendReq().withOptions(AppendReq.Options(sid.stringValue, esr))

      test(c.EventNumber.exact(0L), ExpectedStreamRevision.Revision(0L))
      test(c.StreamRevision.NoStream, ExpectedStreamRevision.NoStream(AppendReq.Empty()))
      test(c.StreamRevision.StreamExists, ExpectedStreamRevision.StreamExists(AppendReq.Empty()))
      test(c.StreamRevision.Any, ExpectedStreamRevision.Any(AppendReq.Empty()))
    }

    "mkAppendProposalsReq" >> {

      import grpc.constants.Metadata.{IsJson, Type}

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
        mkAppendProposalsReq(nel) shouldEqual nel.zipWithIndex.map {
          case (e, i) =>
            AppendReq().withProposedMessage(
              AppendReq
                .ProposedMessage()
                .withId(UUID().withStructured(
                  UUID.Structured(e.eventId.getMostSignificantBits, e.eventId.getLeastSignificantBits)))
                .withMetadata(Map(Type -> s"et-$i", IsJson -> e.isJson.fold("true", "false")))
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

  "incoming" should {

    import incoming._
    import Streams.{DeleteResult, WriteResult}
    import grpc.constants.Metadata.{Created, IsJson, Type}

    type ErrorOr[A] = Either[Throwable, A]

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
      val metadata   = Map(IsJson -> "true", Type -> eventType, Created -> created.getNano().toString)

      val eventProto =
        ReadResp.ReadEvent
          .RecordedEvent()
          .withStreamName(streamId)
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
      val linkMetadata   = Map(IsJson -> "false", Type -> linkEventType, Created -> linkCreated.getNano().toString)

      val linkProto = ReadResp.ReadEvent
        .RecordedEvent()
        .withStreamName(linkStreamId)
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
        c.EventData.json(c.EventType(eventType).unsafe, JUUID.fromString(id), data, customMeta).unsafe,
        created
      )

      val linkRecord = c.EventRecord(
        c.StreamId.from(linkStreamId).unsafe,
        c.EventNumber.exact(linkRevision),
        c.Position.exact(linkCommit, linkPrepare),
        c.EventData.binary(c.EventType.LinkTo, JUUID.fromString(linkId), linkData, linkCustomMeta).unsafe,
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
      val metadata        = Map(IsJson -> "false", Type -> eventType, Created -> created.getNano().toString)

      val recordedEvent =
        ReadResp.ReadEvent
          .RecordedEvent()
          .withStreamName(streamId)
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
        c.EventData.binary(c.EventType(eventType).unsafe, JUUID.fromString(id), data, customMeta).unsafe,
        created
      )

      // Happy Path
      mkEventRecord[ErrorOr](recordedEvent) shouldEqual eventRecord.asRight

      // Bad StreamId
      mkEventRecord[ErrorOr](recordedEvent.withStreamName("")) shouldEqual
        ProtoResultError("name cannot be empty").asLeft

      // Missing UUID
      mkEventRecord[ErrorOr](recordedEvent.withId(UUID().withValue(UUID.Value.Empty))) shouldEqual
        ProtoResultError("UUID is missing").asLeft

      // Bad UUID
      mkEventRecord[ErrorOr](recordedEvent.withId(UUID().withString("invalid"))) shouldEqual
        ProtoResultError("Invalid UUID string: invalid").asLeft

      // Missing EventType
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.filterKeys(_ != Type).toMap)) shouldEqual
        ProtoResultError(s"Required value $Type missing or invalid.").asLeft

      // Missing IsJson
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.filterKeys(_ != IsJson).toMap)) shouldEqual
        ProtoResultError(s"Required value $IsJson missing or invalid.").asLeft

      // Bad IsJson
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.updated(IsJson, "no").toMap)) shouldEqual
        ProtoResultError(s"Required value $IsJson missing or invalid.").asLeft

      // Missing Created
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.filterKeys(_ != Created).toMap)) shouldEqual
        ProtoResultError(s"Required value $Created missing or invalid.").asLeft

      // Bad Created
      mkEventRecord[ErrorOr](recordedEvent.withMetadata(metadata.updated(Created, "chuck norris").toMap)) shouldEqual
        ProtoResultError(s"Required value $Created missing or invalid.").asLeft
    }

    "mkStreamId" >> {
      mkStreamId[ErrorOr](null) shouldEqual ProtoResultError("name cannot be empty").asLeft
      mkStreamId[ErrorOr]("") shouldEqual ProtoResultError("name cannot be empty").asLeft
      mkStreamId[ErrorOr]("abc") shouldEqual c.StreamId.normal("abc").unsafe.asRight
      mkStreamId[ErrorOr]("$abc") shouldEqual c.StreamId.system("abc").unsafe.asRight
      mkStreamId[ErrorOr]("$all") shouldEqual c.StreamId.All.asRight
      mkStreamId[ErrorOr]("$$abc") shouldEqual c.StreamId.MetaId(c.StreamId.normal("abc").unsafe).asRight
      mkStreamId[ErrorOr]("$$$streams") shouldEqual c.StreamId.MetaId(c.StreamId.Streams).asRight
    }

    "mkEventType" >> {
      mkEventType[ErrorOr](null) shouldEqual ProtoResultError("Event type name cannot be empty").asLeft
      mkEventType[ErrorOr]("") shouldEqual ProtoResultError("Event type name cannot be empty").asLeft
      mkEventType[ErrorOr]("sec.protos.A") shouldEqual c.EventType.userDefined("sec.protos.A").unsafe.asRight
      mkEventType[ErrorOr]("$system-type") shouldEqual c.EventType.systemDefined("system-type").unsafe.asRight
      mkEventType[ErrorOr]("$>") shouldEqual c.EventType.LinkTo.asRight
    }

    "mkUUID" >> {
      val uuidString     = "e5390fcb-48bd-4895-bcc3-01629cca2af6"
      val juuid          = JUUID.fromString(uuidString)
      val uuidStructured = UUID.Structured(juuid.getMostSignificantBits(), juuid.getLeastSignificantBits())

      mkUUID[ErrorOr](UUID().withValue(UUID.Value.Empty)) shouldEqual ProtoResultError("UUID is missing").asLeft
      mkUUID[ErrorOr](UUID().withString(uuidString)) shouldEqual juuid.asRight
      mkUUID[ErrorOr](UUID().withStructured(uuidStructured)) shouldEqual juuid.asRight
    }

    "mkWriteResult" >> {
      mkWriteResult[ErrorOr](AppendResp().withCurrentRevision(1L)) shouldEqual
        WriteResult(c.EventNumber.exact(1L)).asRight

      mkWriteResult[ErrorOr](AppendResp().withNoStream(AppendResp.Empty())) shouldEqual
        ProtoResultError("Did not expect NoStream when using NonEmptyList").asLeft

      mkWriteResult[ErrorOr](AppendResp().with_Empty(AppendResp.Empty())) shouldEqual
        ProtoResultError("CurrentRevisionOptions is missing").asLeft
    }

    "mkDeleteResult" >> {
      mkDeleteResult[ErrorOr](DeleteResp().withPosition(DeleteResp.Position(1L, 1L))) shouldEqual
        DeleteResult(c.Position.exact(1L, 1L)).asRight

      mkDeleteResult[ErrorOr](DeleteResp().with_Empty(DeleteResp.Empty())) shouldEqual
        ProtoResultError("Required value DeleteResp.PositionOptions.Position missing or invalid.").asLeft

      mkDeleteResult[ErrorOr](TombstoneResp().withPosition(TombstoneResp.Position(1L, 1L))) shouldEqual
        DeleteResult(c.Position.exact(1L, 1L)).asRight

      mkDeleteResult[ErrorOr](TombstoneResp().with_Empty(TombstoneResp.Empty())) shouldEqual
        ProtoResultError("Required value TombstoneResp.PositionOptions.Position missing or invalid.").asLeft
    }

  }
}
