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

import cats.data.{NonEmptyList, OptionT}
import cats.syntax.all._
import com.eventstore.dbclient.proto.shared._
import com.eventstore.dbclient.proto.streams._
import sec.api.exceptions._
import sec.api.grpc.constants.Metadata.{ContentType, ContentTypes, Created, Type}
import sec.api.mapping.implicits._
import sec.api.mapping.shared._
import sec.api.mapping.time._

private[sec] object streams {

//======================================================================================================================
//                                                     Outgoing
//======================================================================================================================

  object outgoing {

    val empty: Empty = Empty()

    val uuidOption: ReadReq.Options.UUIDOption = ReadReq.Options.UUIDOption().withStructured(empty)

    val mapLogPosition: LogPosition => ReadReq.Options.AllOptions.AllOption = {
      case LogPosition.Exact(c, p) => ReadReq.Options.AllOptions.AllOption.Position(ReadReq.Options.Position(c, p))
      case LogPosition.End         => ReadReq.Options.AllOptions.AllOption.End(empty)
    }

    val mapLogPositionOpt: Option[LogPosition] => ReadReq.Options.AllOptions.AllOption = {
      case Some(v) => mapLogPosition(v)
      case None    => ReadReq.Options.AllOptions.AllOption.Start(empty)
    }

    val mapStreamPosition: StreamPosition => ReadReq.Options.StreamOptions.RevisionOption = {
      case StreamPosition.Exact(nr) => ReadReq.Options.StreamOptions.RevisionOption.Revision(nr)
      case StreamPosition.End       => ReadReq.Options.StreamOptions.RevisionOption.End(empty)
    }

    val mapStreamPositionOpt: Option[StreamPosition] => ReadReq.Options.StreamOptions.RevisionOption = {
      case Some(v) => mapStreamPosition(v)
      case None    => ReadReq.Options.StreamOptions.RevisionOption.Start(empty)
    }

    val mapDirection: Direction => ReadReq.Options.ReadDirection = {
      case Direction.Forwards  => ReadReq.Options.ReadDirection.Forwards
      case Direction.Backwards => ReadReq.Options.ReadDirection.Backwards
    }

    val mapReadEventFilter: Option[SubscriptionFilterOptions] => ReadReq.Options.FilterOption = {

      import ReadReq.Options.FilterOption

      def filter(options: SubscriptionFilterOptions): FilterOption = {

        val expr = options.filter.option.fold(
          nel => ReadReq.Options.FilterOptions.Expression().withPrefix(nel.map(_.value).toList),
          reg => ReadReq.Options.FilterOptions.Expression().withRegex(reg.value)
        )

        val window = options.maxSearchWindow
          .map(ReadReq.Options.FilterOptions.Window.Max)
          .getOrElse(ReadReq.Options.FilterOptions.Window.Count(empty))

        val filterOptions = ReadReq.Options
          .FilterOptions()
          .withWindow(window)
          .withCheckpointIntervalMultiplier(options.checkpointIntervalMultiplier)

        val result = options.filter.kind match {
          case EventFilter.ByStreamId  => filterOptions.withStreamIdentifier(expr)
          case EventFilter.ByEventType => filterOptions.withEventType(expr)
        }

        FilterOption.Filter(result)
      }

      def noFilter: FilterOption = FilterOption.NoFilter(empty)

      _.fold(noFilter)(filter)
    }

    def mkSubscribeToStreamReq(
      streamId: StreamId,
      exclusiveFrom: Option[StreamPosition],
      resolveLinkTos: Boolean
    ): ReadReq = {

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(streamId.esSid.some, mapStreamPositionOpt(exclusiveFrom)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(Direction.Forwards))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(empty)
        .withUuidOption(uuidOption)

      ReadReq().withOptions(options)
    }

    def mkSubscribeToAllReq(
      exclusiveFrom: Option[LogPosition],
      resolveLinkTos: Boolean,
      filterOptions: Option[SubscriptionFilterOptions]
    ): ReadReq = {

      val options = ReadReq
        .Options()
        .withAll(ReadReq.Options.AllOptions(mapLogPositionOpt(exclusiveFrom)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(Direction.Forwards))
        .withResolveLinks(resolveLinkTos)
        .withFilterOption(mapReadEventFilter(filterOptions))
        .withUuidOption(uuidOption)

      ReadReq().withOptions(options)
    }

    def mkReadStreamReq(
      streamId: StreamId,
      from: StreamPosition,
      direction: Direction,
      count: Long,
      resolveLinkTos: Boolean
    ): ReadReq = {

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(streamId.esSid.some, mapStreamPosition(from)))
        .withCount(count)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(empty)
        .withUuidOption(uuidOption)

      ReadReq().withOptions(options)
    }

    def mkReadAllReq(
      from: LogPosition,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean
    ): ReadReq = {

      val options = ReadReq
        .Options()
        .withAll(ReadReq.Options.AllOptions(mapLogPosition(from)))
        .withCount(maxCount)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(empty)
        .withUuidOption(uuidOption)

      ReadReq().withOptions(options)
    }

    def mkDeleteReq(streamId: StreamId, expectedState: StreamState): DeleteReq = {

      val mapDeleteExpected: StreamState => DeleteReq.Options.ExpectedStreamRevision = {
        case StreamPosition.Exact(v)  => DeleteReq.Options.ExpectedStreamRevision.Revision(v)
        case StreamState.NoStream     => DeleteReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamState.StreamExists => DeleteReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamState.Any          => DeleteReq.Options.ExpectedStreamRevision.Any(empty)
      }
      DeleteReq().withOptions(DeleteReq.Options(streamId.esSid.some, mapDeleteExpected(expectedState)))
    }

    def mkTombstoneReq(streamId: StreamId, expectedState: StreamState): TombstoneReq = {

      val mapTombstoneExpected: StreamState => TombstoneReq.Options.ExpectedStreamRevision = {
        case StreamPosition.Exact(v)  => TombstoneReq.Options.ExpectedStreamRevision.Revision(v)
        case StreamState.NoStream     => TombstoneReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamState.StreamExists => TombstoneReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamState.Any          => TombstoneReq.Options.ExpectedStreamRevision.Any(empty)
      }
      TombstoneReq().withOptions(TombstoneReq.Options(streamId.esSid.some, mapTombstoneExpected(expectedState)))
    }

    def mkAppendHeaderReq(streamId: StreamId, expectedState: StreamState): AppendReq = {

      val mapAppendExpected: StreamState => AppendReq.Options.ExpectedStreamRevision = {
        case StreamPosition.Exact(v)  => AppendReq.Options.ExpectedStreamRevision.Revision(v)
        case StreamState.NoStream     => AppendReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamState.StreamExists => AppendReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamState.Any          => AppendReq.Options.ExpectedStreamRevision.Any(empty)
      }
      AppendReq().withOptions(AppendReq.Options(streamId.esSid.some, mapAppendExpected(expectedState)))
    }

    def mkAppendProposalsReq(events: NonEmptyList[EventData]): NonEmptyList[AppendReq] =
      events.map { e =>
        val id         = mkUuid(e.eventId)
        val customMeta = e.metadata.toByteString
        val data       = e.data.toByteString
        val ct         = e.contentType.fold(ContentTypes.ApplicationOctetStream, ContentTypes.ApplicationJson)
        val meta       = Map(Type -> EventType.eventTypeToString(e.eventType), ContentType -> ct)
        AppendReq().withProposedMessage(AppendReq.ProposedMessage(id.some, meta, customMeta, data))
      }
  }

//======================================================================================================================
//                                                     Incoming
//======================================================================================================================

  object incoming {

    def mkPositionGlobal[F[_]: ErrorA](e: ReadResp.ReadEvent.RecordedEvent): F[PositionInfo.Global] =
      (mkStreamPosition[F](e), mkLogPosition[F](e)).mapN(PositionInfo.Global)

    def mkLogPosition[F[_]: ErrorA](e: ReadResp.ReadEvent.RecordedEvent): F[LogPosition.Exact] =
      LogPosition(e.commitPosition, e.preparePosition).liftTo[F]

    def mkStreamPosition[F[_]: ErrorA](e: ReadResp.ReadEvent.RecordedEvent): F[StreamPosition.Exact] =
      StreamPosition(e.streamRevision).liftTo[F]

    def mkCheckpoint[F[_]: ErrorA](c: ReadResp.Checkpoint): F[Checkpoint] =
      LogPosition(c.commitPosition, c.preparePosition)
        .map(Checkpoint)
        .leftMap(error => ProtoResultError(s"Invalid position for Checkpoint: ${error.msg}"))
        .liftTo[F]

    def mkCheckpointOrEvent[F[_]: ErrorM](re: ReadResp): F[Option[Either[Checkpoint, AllEvent]]] = {

      val mkEvt      = mkEvent[F, PositionInfo.Global](_, mkPositionGlobal[F])
      val event      = OptionT(re.content.event.flatTraverse(mkEvt).nested.map(_.asRight[Checkpoint]).value)
      val checkpoint = OptionT(re.content.checkpoint.traverse(mkCheckpoint[F]).nested.map(_.asLeft[AllEvent]).value)

      (event <+> checkpoint).value
    }

    def mkStreamNotFound[F[_]: ErrorM](snf: ReadResp.StreamNotFound): F[StreamNotFound] =
      snf.streamIdentifier.require[F]("StreamIdentifer") >>= (_.utf8[F].map(StreamNotFound))

    def failStreamNotFound[F[_]: ErrorM](rr: ReadResp): F[ReadResp] =
      rr.content.streamNotFound.fold(rr.pure[F])(mkStreamNotFound[F](_) >>= (_.raiseError[F, ReadResp]))

    def reqReadAll[F[_]: ErrorM](rr: ReadResp): F[Option[AllEvent]] =
      reqReadEvent(rr, mkPositionGlobal[F])

    def reqReadStream[F[_]: ErrorM](rr: ReadResp): F[Option[StreamEvent]] =
      reqReadEvent(rr, mkStreamPosition[F])

    def reqReadEvent[F[_]: ErrorM, P <: PositionInfo](
      rr: ReadResp,
      mkPos: ReadResp.ReadEvent.RecordedEvent => F[P]): F[Option[Event[P]]] =
      rr.content.event.require[F]("ReadEvent") >>= (mkEvent[F, P](_, mkPos))

    def reqConfirmation[F[_]: ErrorA](rr: ReadResp): F[SubscriptionConfirmation] =
      rr.content.confirmation
        .map(_.subscriptionId)
        .require[F]("SubscriptionConfirmation", details = s"Got ${rr.content} instead".some)
        .map(SubscriptionConfirmation)

    def mkEvent[F[_]: ErrorM, P <: PositionInfo](
      re: ReadResp.ReadEvent,
      mkPos: ReadResp.ReadEvent.RecordedEvent => F[P]
    ): F[Option[Event[P]]] =
      re.event.traverse(e => mkEventRecord[F, P](e, mkPos)) >>= { eOpt =>
        re.link
          .traverse(e => mkEventRecord[F, P](e, mkPos))
          .map(lOpt => eOpt.map(er => lOpt.fold[Event[P]](er)(ResolvedEvent[P](er, _))))
      }

    def mkEventRecord[F[_]: ErrorM, P <: PositionInfo](
      e: ReadResp.ReadEvent.RecordedEvent,
      mkPos: ReadResp.ReadEvent.RecordedEvent => F[P]): F[EventRecord[P]] = {

      val streamId    = mkStreamId[F](e.streamIdentifier)
      val position    = mkPos(e)
      val data        = e.data.toByteVector
      val customMeta  = e.customMetadata.toByteVector
      val eventId     = e.id.require[F]("UUID") >>= mkJuuid[F]
      val eventType   = e.metadata.get(Type).require[F](Type) >>= mkEventType[F]
      val contentType = e.metadata.get(ContentType).require[F](ContentType) >>= mkContentType[F]
      val created     = e.metadata.get(Created).flatMap(_.toLongOption).require[F](Created) >>= fromTicksSinceEpoch[F]
      val eventData   = (eventType, eventId, contentType).mapN((t, i, ct) => EventData(t, i, data, customMeta, ct))

      (streamId, position, eventData, created).mapN((id, p, ed, c) => sec.EventRecord(id, p, ed, c))

    }

    def mkContentType[F[_]](ct: String)(implicit F: ErrorA[F]): F[ContentType] =
      ct match {
        case ContentTypes.ApplicationOctetStream => F.pure(sec.ContentType.Binary)
        case ContentTypes.ApplicationJson        => F.pure(sec.ContentType.Json)
        case unknown                             => F.raiseError(ProtoResultError(s"Required value $ContentType missing or invalid: $unknown"))
      }

    def mkEventType[F[_]: ErrorA](name: String): F[EventType] =
      EventType.stringToEventType(Option(name).getOrElse("")).leftMap(ProtoResultError).liftTo[F]

    def mkWriteResult[F[_]: ErrorA](sid: StreamId, ar: AppendResp): F[WriteResult] = {

      import AppendResp.{Result, Success}
      import AppendResp.WrongExpectedVersion.ExpectedRevisionOption
      import AppendResp.WrongExpectedVersion.CurrentRevisionOption

      def error[T](msg: String): Either[Throwable, T] =
        ProtoResultError(msg).asLeft[T]

      def success(s: Result.Success) = {

        val logPositionExact: Either[Throwable, LogPosition.Exact] = s.value.positionOption match {
          case Success.PositionOption.Position(p)   => LogPosition.exact(p.commitPosition, p.preparePosition).asRight
          case Success.PositionOption.NoPosition(_) => error("Did not expect NoPosition when using NonEmptyList")
          case Success.PositionOption.Empty         => error("PositionOption is missing")
        }

        val streamPositionExact: Either[Throwable, StreamPosition.Exact] = s.value.currentRevisionOption match {
          case Success.CurrentRevisionOption.CurrentRevision(v) => StreamPosition.exact(v).asRight
          case Success.CurrentRevisionOption.NoStream(_)        => error("Did not expect NoStream when using NonEmptyList")
          case Success.CurrentRevisionOption.Empty              => error("CurrentRevisionOption is missing")
        }

        (streamPositionExact, logPositionExact).mapN((r, p) => WriteResult(r, p))

      }

      def wrongExpectedStreamState(w: Result.WrongExpectedVersion) = {

        val expected: Either[Throwable, StreamState] = w.value.expectedRevisionOption match {
          case ExpectedRevisionOption.ExpectedRevision(v)     => StreamPosition.exact(v).asRight
          case ExpectedRevisionOption.ExpectedNoStream(_)     => StreamState.NoStream.asRight
          case ExpectedRevisionOption.ExpectedAny(_)          => StreamState.Any.asRight
          case ExpectedRevisionOption.ExpectedStreamExists(_) => StreamState.StreamExists.asRight
          case ExpectedRevisionOption.Empty                   => error("ExpectedRevisionOption is missing")
        }

        val actual: Either[Throwable, StreamState] = w.value.currentRevisionOption match {
          case CurrentRevisionOption.CurrentRevision(v) => StreamPosition.exact(v).asRight
          case CurrentRevisionOption.CurrentNoStream(_) => StreamState.NoStream.asRight
          case CurrentRevisionOption.Empty              => error("CurrentRevisionOption is missing")
        }

        (expected, actual).mapN((e, a) => WrongExpectedState(sid, e, a))
      }

      val result: Either[Throwable, WriteResult] = ar.result match {
        case s: Result.Success              => success(s)
        case w: Result.WrongExpectedVersion => wrongExpectedStreamState(w) >>= (_.asLeft[WriteResult])
        case Result.Empty                   => error("Result is missing")
      }

      result.liftTo[F]
    }

    def mkDeleteResult[F[_]: ErrorA](dr: DeleteResp): F[DeleteResult] =
      dr.positionOption.position
        .map(p => DeleteResult(LogPosition.exact(p.commitPosition, p.preparePosition)))
        .require[F]("DeleteResp.PositionOptions.Position")

    def mkTombstoneResult[F[_]: ErrorA](tr: TombstoneResp): F[TombstoneResult] =
      tr.positionOption.position
        .map(p => TombstoneResult(LogPosition.exact(p.commitPosition, p.preparePosition)))
        .require[F]("TombstoneResp.PositionOptions.Position")

  }

}
