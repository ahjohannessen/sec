package sec
package api
package mapping

import java.util.{UUID => JUUID}
import cats.data.NonEmptyList
import cats.implicits._
import com.eventstore.client.streams._
import sec.core._
import sec.api.Streams._
import grpc.constants.Metadata.{IsJson, Type}

private[sec] object streams {

//======================================================================================================================
//                                                     Outgoing
//======================================================================================================================

  object outgoing {

    val mapPositionOpt: Option[Position] => ReadReq.Options.AllOptions.AllOptions = {
      case Some(v) => mapPosition(v)
      case None    => ReadReq.Options.AllOptions.AllOptions.Start(ReadReq.Empty())
    }

    val mapPosition: Position => ReadReq.Options.AllOptions.AllOptions = {
      case Position.Exact(c, p) => ReadReq.Options.AllOptions.AllOptions.Position(ReadReq.Options.Position(c, p))
      case Position.End         => ReadReq.Options.AllOptions.AllOptions.End(ReadReq.Empty())
    }

    val mapEventNumberOpt: Option[EventNumber] => ReadReq.Options.StreamOptions.RevisionOptions = {
      case Some(v) => mapEventNumber(v)
      case None    => ReadReq.Options.StreamOptions.RevisionOptions.Start(ReadReq.Empty())
    }

    val mapEventNumber: EventNumber => ReadReq.Options.StreamOptions.RevisionOptions = {
      case EventNumber.Exact(nr) => ReadReq.Options.StreamOptions.RevisionOptions.Revision(nr)
      case EventNumber.End       => ReadReq.Options.StreamOptions.RevisionOptions.End(ReadReq.Empty())
    }

    val mapDirection: ReadDirection => ReadReq.Options.ReadDirection = {
      case ReadDirection.Forward  => ReadReq.Options.ReadDirection.Forwards
      case ReadDirection.Backward => ReadReq.Options.ReadDirection.Backwards
    }

    val mapReadEventFilter: Option[EventFilter] => ReadReq.Options.FilterOptionsOneof = {

      import ReadReq.Options.FilterOptionsOneof

      def filter(filter: EventFilter): FilterOptionsOneof = {

        val expr = filter.option.fold(
          nel => ReadReq.Options.FilterOptions.Expression().withPrefix(nel.map(_.value).toList),
          reg => ReadReq.Options.FilterOptions.Expression().withRegex(reg.value)
        )

        val window = filter.maxSearchWindow
          .map(ReadReq.Options.FilterOptions.Window.Max)
          .getOrElse(ReadReq.Options.FilterOptions.Window.Count(ReadReq.Empty()))

        val result = filter.kind match {
          case EventFilter.StreamName => ReadReq.Options.FilterOptions().withStreamName(expr).withWindow(window)
          case EventFilter.EventType  => ReadReq.Options.FilterOptions().withEventType(expr).withWindow(window)
        }

        FilterOptionsOneof.Filter(result)
      }

      def noFilter: FilterOptionsOneof = FilterOptionsOneof.NoFilter(ReadReq.Empty())

      _.fold(noFilter)(filter)
    }

    def mkSubscribeToStreamReq(
      stream: String,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean
    ): ReadReq = {

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(stream, mapEventNumberOpt(exclusiveFrom)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(ReadDirection.Forward))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(ReadReq.Empty())

      ReadReq().withOptions(options)
    }

    def mkSubscribeToAllReq(
      exclusiveFrom: Option[Position],
      resolveLinkTos: Boolean,
      filter: Option[EventFilter]
    ): ReadReq = {

      val options = ReadReq
        .Options()
        .withAll(ReadReq.Options.AllOptions(mapPositionOpt(exclusiveFrom)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(ReadDirection.Forward))
        .withResolveLinks(resolveLinkTos)
        .withFilterOptionsOneof(mapReadEventFilter(filter))

      ReadReq().withOptions(options)
    }

    def mkReadAllReq(
      position: Position,
      direction: ReadDirection,
      maxCount: Int,
      resolveLinkTos: Boolean,
      filter: Option[EventFilter]
    ): ReadReq = {

      val options = ReadReq
        .Options()
        .withAll(ReadReq.Options.AllOptions(mapPosition(position)))
        .withCount(maxCount)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withFilterOptionsOneof(mapReadEventFilter(filter))

      ReadReq().withOptions(options)
    }

    def mkReadStreamReq(
      stream: String,
      direction: ReadDirection,
      from: EventNumber,
      count: Int,
      resolveLinkTos: Boolean
    ): ReadReq = {

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(stream, mapEventNumber(from)))
        .withCount(count)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(ReadReq.Empty())

      ReadReq().withOptions(options)
    }

    def mkSoftDeleteReq(stream: String, expectedRevision: StreamRevision): DeleteReq = {
      def empty = DeleteReq.Empty()
      val mapDeleteRevision: StreamRevision => DeleteReq.Options.ExpectedStreamRevision = {
        case EventNumber.Exact(v)        => DeleteReq.Options.ExpectedStreamRevision.Revision(v)
        case StreamRevision.NoStream     => DeleteReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamRevision.StreamExists => DeleteReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamRevision.Any          => DeleteReq.Options.ExpectedStreamRevision.Any(empty)
      }
      DeleteReq().withOptions(DeleteReq.Options(stream, mapDeleteRevision(expectedRevision)))
    }

    def mkHardDeleteReq(stream: String, expectedRevision: StreamRevision): TombstoneReq = {
      def empty = TombstoneReq.Empty()
      val mapTombstoneRevision: StreamRevision => TombstoneReq.Options.ExpectedStreamRevision = {
        case EventNumber.Exact(v)        => TombstoneReq.Options.ExpectedStreamRevision.Revision(v)
        case StreamRevision.NoStream     => TombstoneReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamRevision.StreamExists => TombstoneReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamRevision.Any          => TombstoneReq.Options.ExpectedStreamRevision.Any(empty)
      }
      TombstoneReq().withOptions(TombstoneReq.Options(stream, mapTombstoneRevision(expectedRevision)))
    }

    def mkAppendHeaderReq(stream: String, expectedRevision: StreamRevision): AppendReq = {
      def empty = AppendReq.Empty()
      val mapAppendRevision: StreamRevision => AppendReq.Options.ExpectedStreamRevision = {
        case EventNumber.Exact(v)        => AppendReq.Options.ExpectedStreamRevision.Revision(v)
        case StreamRevision.NoStream     => AppendReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamRevision.StreamExists => AppendReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamRevision.Any          => AppendReq.Options.ExpectedStreamRevision.Any(empty)
      }
      AppendReq().withOptions(AppendReq.Options(stream, mapAppendRevision(expectedRevision)))
    }

    def mkAppendProposalsReq(events: NonEmptyList[EventData]): NonEmptyList[AppendReq] = events.map { e =>
      val id         = UUID().withString(e.eventId.toString)
      val customMeta = e.metadata.bytes.toByteString
      val data       = e.data.bytes.toByteString
      val meta       = Map(Type -> EventType.toStr(e.eventType), IsJson -> e.isJson.fold("true", "false"))
      val proposal   = AppendReq.ProposedMessage(id.some, meta, customMeta, data)
      AppendReq().withProposedMessage(proposal)
    }
  }

//======================================================================================================================
//                                                     Incoming
//======================================================================================================================

  object incoming {

    def mkEvent[F[_]: ErrorM](re: ReadResp.ReadEvent): F[Option[Event]] =
      re.event.traverse(mkEventRecord[F]) >>= { eOpt =>
        re.link.traverse(mkEventRecord[F]).map { lOpt =>
          eOpt.map(er => lOpt.fold[Event](er)(ResolvedEvent(er, _)))
        }
      }

    def mkEventRecord[F[_]: ErrorM](e: ReadResp.ReadEvent.RecordedEvent): F[EventRecord] = {

      import EventData.{binary, json}
      import grpc.constants.Metadata.{Created, IsJson, Type}

      val streamId    = e.streamName
      val eventNumber = EventNumber.exact(e.streamRevision)
      val position    = Position.exact(e.commitPosition, e.preparePosition)
      val data        = e.data.toByteVector
      val customMeta  = e.customMetadata.toByteVector
      val eventId     = e.id.require[F]("UUID") >>= mkUUID[F]
      val eventType   = e.metadata.get(Type).require[F](Type) >>= mkEventType[F]
      val isJson      = e.metadata.get(IsJson).flatMap(_.toBooleanOption).require[F](IsJson)
      val created     = e.metadata.get(Created).flatMap(_.toLongOption).require[F](Created) >>= fromTicksSinceEpoch[F]

      val eventData = (eventId, eventType, isJson).mapN { (i, t, j) =>
        j.fold(json(t, i, data, customMeta), binary(t, i, data, customMeta)).leftMap(ProtoResultError).liftTo[F]
      }

      (eventData.flatten, created).mapN((ed, c) => EventRecord(streamId, eventNumber, position, ed, c))

    }

    def mkEventType[F[_]: ErrorA](name: String): F[EventType] =
      EventType.fromStr(name).leftMap(ProtoResultError).liftTo[F]

    def mkUUID[F[_]: ErrorA](uuid: UUID): F[JUUID] = {

      val juuid = uuid.value match {
        case UUID.Value.Structured(v) => new JUUID(v.mostSignificantBits, v.leastSignificantBits).asRight
        case UUID.Value.String(v)     => JUUID.fromString(v).asRight
        case UUID.Value.Empty         => "UUID is missing".asLeft
      }

      juuid.leftMap(ProtoResultError).liftTo[F]
    }

    def mkWriteResult[F[_]: ErrorA](ar: AppendResp): F[WriteResult] = {

      val currentRevision = ar.currentRevisionOptions match {
        case AppendResp.CurrentRevisionOptions.CurrentRevision(v) => EventNumber.exact(v).asRight
        case AppendResp.CurrentRevisionOptions.NoStream(_)        => "Did not expect NoStream when using NonEmptyList".asLeft
        case AppendResp.CurrentRevisionOptions.Empty              => "CurrentRevisionOptions is missing".asLeft
      }

      currentRevision.map(WriteResult).leftMap(ProtoResultError).liftTo[F]
    }

    def mkDeleteResult[F[_]: ErrorA](dr: DeleteResp): F[DeleteResult] =
      dr.positionOptions.position
        .map(p => DeleteResult(Position.exact(p.commitPosition, p.preparePosition)))
        .require[F]("DeleteResp.PositionOptions.Position")

    def mkDeleteResult[F[_]: ErrorA](tr: TombstoneResp): F[DeleteResult] =
      tr.positionOptions.position
        .map(p => DeleteResult(Position.exact(p.commitPosition, p.preparePosition)))
        .require[F]("TombstoneResp.PositionOptions.Position")

  }

}
