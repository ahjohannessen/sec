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
package mapping

import java.util.UUID as JUUID
import java.time.Instant
import java.util.concurrent.TimeoutException
import cats.{Applicative, ApplicativeThrow, MonadThrow}
import cats.data.{NonEmptyList, OptionT}
import cats.syntax.all.*
import io.kurrent.dbclient.proto.shared as pshared
import io.kurrent.dbclient.proto.streams.*
import sec.api.exceptions.*
import sec.api.grpc.constants.Metadata.{ContentType, ContentTypes, Created, Type}
import sec.api.mapping.shared.*
import sec.api.mapping.time.*

private[sec] object streams:

//======================================================================================================================
//                                                     Outgoing
//======================================================================================================================

  object outgoing:

    val empty: pshared.Empty = pshared.Empty()

    val uuidOption: ReadReq.Options.UUIDOption = ReadReq.Options.UUIDOption().withStructured(empty)

    val mapLogPosition: LogPosition => ReadReq.Options.AllOptions.AllOption =
      case LogPosition.Exact(c, p) =>
        ReadReq.Options.AllOptions.AllOption.Position(ReadReq.Options.Position(c.toLong, p.toLong))
      case LogPosition.End => ReadReq.Options.AllOptions.AllOption.End(empty)

    val mapLogPositionOpt: Option[LogPosition] => ReadReq.Options.AllOptions.AllOption =
      case Some(v) => mapLogPosition(v)
      case None    => ReadReq.Options.AllOptions.AllOption.Start(empty)

    val mapStreamPosition: StreamPosition => ReadReq.Options.StreamOptions.RevisionOption =
      case StreamPosition.Exact(nr) => ReadReq.Options.StreamOptions.RevisionOption.Revision(nr.toLong)
      case StreamPosition.End       => ReadReq.Options.StreamOptions.RevisionOption.End(empty)

    val mapStreamPositionOpt: Option[StreamPosition] => ReadReq.Options.StreamOptions.RevisionOption =
      case Some(v) => mapStreamPosition(v)
      case None    => ReadReq.Options.StreamOptions.RevisionOption.Start(empty)

    val mapDirection: Direction => ReadReq.Options.ReadDirection =
      case Direction.Forwards  => ReadReq.Options.ReadDirection.Forwards
      case Direction.Backwards => ReadReq.Options.ReadDirection.Backwards

    val mapReadEventFilter: Option[SubscriptionFilterOptions] => ReadReq.Options.FilterOption =

      import ReadReq.Options.FilterOption

      def filter(options: SubscriptionFilterOptions): FilterOption =

        val expr = options.filter.option.fold(
          nel => ReadReq.Options.FilterOptions.Expression().withPrefix(nel.map(_.value).toList),
          reg => ReadReq.Options.FilterOptions.Expression().withRegex(reg.value)
        )

        val window = options.maxSearchWindow
          .map(ReadReq.Options.FilterOptions.Window.Max(_))
          .getOrElse(ReadReq.Options.FilterOptions.Window.Count(empty))

        val filterOptions = ReadReq.Options
          .FilterOptions()
          .withWindow(window)
          .withCheckpointIntervalMultiplier(options.checkpointIntervalMultiplier)

        val result = options.filter.kind match
          case EventFilter.ByStreamId  => filterOptions.withStreamIdentifier(expr)
          case EventFilter.ByEventType => filterOptions.withEventType(expr)

        FilterOption.Filter(result)

      def noFilter: FilterOption = FilterOption.NoFilter(empty)

      _.fold(noFilter)(filter)

    def mkSubscribeToStreamReq(
      streamId: StreamId,
      exclusiveFrom: Option[StreamPosition],
      resolveLinkTos: Boolean
    ): ReadReq =

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(streamId.esSid.some, mapStreamPositionOpt(exclusiveFrom)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(Direction.Forwards))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(empty)
        .withUuidOption(uuidOption)

      ReadReq().withOptions(options)

    def mkSubscribeToAllReq(
      exclusiveFrom: Option[LogPosition],
      resolveLinkTos: Boolean,
      filterOptions: Option[SubscriptionFilterOptions]
    ): ReadReq =

      val options = ReadReq
        .Options()
        .withAll(ReadReq.Options.AllOptions(mapLogPositionOpt(exclusiveFrom)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(Direction.Forwards))
        .withResolveLinks(resolveLinkTos)
        .withFilterOption(mapReadEventFilter(filterOptions))
        .withUuidOption(uuidOption)

      ReadReq().withOptions(options)

    def mkReadStreamReq(
      streamId: StreamId,
      from: StreamPosition,
      direction: Direction,
      count: Long,
      resolveLinkTos: Boolean
    ): ReadReq =

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(streamId.esSid.some, mapStreamPosition(from)))
        .withCount(count)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(empty)
        .withUuidOption(uuidOption)
        .withControlOption(ReadReq.Options.ControlOption(1))

      ReadReq().withOptions(options)

    def mkReadAllReq(
      from: LogPosition,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean
    ): ReadReq =

      val options = ReadReq
        .Options()
        .withAll(ReadReq.Options.AllOptions(mapLogPosition(from)))
        .withCount(maxCount)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(empty)
        .withUuidOption(uuidOption)
        .withControlOption(ReadReq.Options.ControlOption(1))

      ReadReq().withOptions(options)

    def mkDeleteReq(streamId: StreamId, expectedState: StreamState): DeleteReq =

      val mapDeleteExpected: StreamState => DeleteReq.Options.ExpectedStreamRevision =
        case StreamPosition.Exact(v)  => DeleteReq.Options.ExpectedStreamRevision.Revision(v.toLong)
        case StreamState.NoStream     => DeleteReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamState.StreamExists => DeleteReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamState.Any          => DeleteReq.Options.ExpectedStreamRevision.Any(empty)

      DeleteReq().withOptions(DeleteReq.Options(streamId.esSid.some, mapDeleteExpected(expectedState)))

    def mkTombstoneReq(streamId: StreamId, expectedState: StreamState): TombstoneReq =

      val mapTombstoneExpected: StreamState => TombstoneReq.Options.ExpectedStreamRevision =
        case StreamPosition.Exact(v)  => TombstoneReq.Options.ExpectedStreamRevision.Revision(v.toLong)
        case StreamState.NoStream     => TombstoneReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamState.StreamExists => TombstoneReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamState.Any          => TombstoneReq.Options.ExpectedStreamRevision.Any(empty)

      TombstoneReq().withOptions(TombstoneReq.Options(streamId.esSid.some, mapTombstoneExpected(expectedState)))

    def mkAppendHeaderReq(streamId: StreamId, expectedState: StreamState): AppendReq =

      val mapAppendExpected: StreamState => AppendReq.Options.ExpectedStreamRevision =
        case StreamPosition.Exact(v)  => AppendReq.Options.ExpectedStreamRevision.Revision(v.toLong)
        case StreamState.NoStream     => AppendReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamState.StreamExists => AppendReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamState.Any          => AppendReq.Options.ExpectedStreamRevision.Any(empty)

      AppendReq().withOptions(AppendReq.Options(streamId.esSid.some, mapAppendExpected(expectedState)))

    def mkAppendProposalsReq(events: NonEmptyList[EventData]): NonEmptyList[AppendReq] =
      events.map { e =>
        val id         = mkUuid(e.eventId)
        val customMeta = e.metadata.toByteString
        val data       = e.data.toByteString
        val ct         = e.contentType.fold(ContentTypes.ApplicationOctetStream, ContentTypes.ApplicationJson)
        val meta       = Map(Type -> EventType.eventTypeToString(e.eventType), ContentType -> ct)
        AppendReq().withProposedMessage(AppendReq.ProposedMessage(id.some, meta, customMeta, data))
      }

    def mkBatchAppendHeader(
      streamId: StreamId,
      expectedState: StreamState,
      deadline: Option[Instant]
    ): BatchAppendReq.Options =

      import com.google.protobuf.timestamp.Timestamp

      val empty = com.google.protobuf.empty.Empty()

      val mapExpectedStreamPosition: StreamState => BatchAppendReq.Options.ExpectedStreamPosition =
        case StreamPosition.Exact(v)  => BatchAppendReq.Options.ExpectedStreamPosition.StreamPosition(v.toLong)
        case StreamState.NoStream     => BatchAppendReq.Options.ExpectedStreamPosition.NoStream(empty)
        case StreamState.StreamExists => BatchAppendReq.Options.ExpectedStreamPosition.StreamExists(empty)
        case StreamState.Any          => BatchAppendReq.Options.ExpectedStreamPosition.Any(empty)

      val req = BatchAppendReq
        .Options()
        .withStreamIdentifier(streamId.esSid)
        .withExpectedStreamPosition(mapExpectedStreamPosition(expectedState))

      deadline.fold(req)(i => req.withDeadline(Timestamp.of(i.getEpochSecond, i.getNano)))

    def mkBatchAppendProposal(e: EventData): BatchAppendReq.ProposedMessage =

      val id         = mkUuid(e.eventId)
      val customMeta = e.metadata.toByteString
      val data       = e.data.toByteString
      val ct         = e.contentType.fold(ContentTypes.ApplicationOctetStream, ContentTypes.ApplicationJson)
      val meta       = Map(Type -> EventType.eventTypeToString(e.eventType), ContentType -> ct)

      BatchAppendReq.ProposedMessage(id.some, meta, customMeta, data)

    def mkHeaderReq(
      correlationId: JUUID,
      streamId: StreamId,
      expectedState: StreamState,
      deadline: Option[Instant]
    ): BatchAppendReq =
      BatchAppendReq()
        .withCorrelationId(mkUuid(correlationId))
        .withOptions(mkBatchAppendHeader(streamId, expectedState, deadline))

    def mkProposalsReq(
      correlationId: JUUID,
      data: List[BatchAppendReq.ProposedMessage],
      isFinal: Boolean
    ): BatchAppendReq =
      BatchAppendReq()
        .withCorrelationId(mkUuid(correlationId))
        .withProposedMessages(data)
        .withIsFinal(isFinal)

  end outgoing

//======================================================================================================================
//                                                     Incoming
//======================================================================================================================

  object incoming:

    def mkLogPosition[F[_]: ApplicativeThrow](e: ReadResp.ReadEvent.RecordedEvent): F[LogPosition.Exact] =
      LogPosition(e.commitPosition, e.preparePosition).liftTo[F]

    def mkStreamPosition[F[_]: Applicative](e: ReadResp.ReadEvent.RecordedEvent): F[StreamPosition.Exact] =
      StreamPosition(e.streamRevision).pure[F]

    def mkCheckpoint[F[_]: ApplicativeThrow](c: ReadResp.Checkpoint): F[Checkpoint] =
      LogPosition(c.commitPosition, c.preparePosition)
        .map(Checkpoint(_))
        .leftMap(error => ProtoResultError(s"Invalid position for Checkpoint: ${error.msg}"))
        .liftTo[F]

    def mkCheckpointOrEvent[F[_]: MonadThrow](re: ReadResp): F[Option[Either[Checkpoint, Event]]] =

      val event      = OptionT(re.content.event.flatTraverse(mkEvent[F]).nested.map(_.asRight[Checkpoint]).value)
      val checkpoint = OptionT(re.content.checkpoint.traverse(mkCheckpoint[F]).nested.map(_.asLeft[Event]).value)

      (event <+> checkpoint).value

    def mkStreamNotFound[F[_]: MonadThrow](snf: ReadResp.StreamNotFound): F[StreamNotFound] =
      snf.streamIdentifier.require[F]("StreamIdentifer") >>= (_.utf8[F].map(StreamNotFound(_)))

    def failStreamNotFound[F[_]: MonadThrow](rr: ReadResp): F[ReadResp] =
      rr.content.streamNotFound.fold(rr.pure[F])(mkStreamNotFound[F](_) >>= (_.raiseError[F, ReadResp]))

    def reqReadEvent[F[_]: MonadThrow](rr: ReadResp): F[Option[Event]] =
      rr.content.event.require[F]("ReadEvent") >>= (mkEvent[F])

    def reqConfirmation[F[_]: ApplicativeThrow](rr: ReadResp): F[SubscriptionConfirmation] =
      rr.content.confirmation
        .map(_.subscriptionId)
        .require[F]("SubscriptionConfirmation", details = s"Got ${rr.content} instead".some)
        .map(SubscriptionConfirmation(_))

    def mkEvent[F[_]: MonadThrow](re: ReadResp.ReadEvent): F[Option[Event]] =
      re.event.traverse(mkEventRecord[F]) >>= { eOpt =>
        re.link
          .traverse(mkEventRecord[F])
          .map(lOpt => eOpt.map(er => lOpt.fold[Event](er)(ResolvedEvent(er, _))))
      }

    def mkEventRecord[F[_]: MonadThrow](e: ReadResp.ReadEvent.RecordedEvent): F[EventRecord] =

      val streamId    = mkStreamId[F](e.streamIdentifier)
      val position    = mkStreamPosition[F](e)
      val logPosition = mkLogPosition[F](e)
      val data        = e.data.toByteVector
      val customMeta  = e.customMetadata.toByteVector
      val eventId     = e.id.require[F]("UUID") >>= mkJuuid[F]
      val eventType   = e.metadata.get(Type).require[F](Type) >>= mkEventType[F]
      val contentType = e.metadata.get(ContentType).require[F](ContentType) >>= mkContentType[F]
      val created     = e.metadata.get(Created).flatMap(_.toLongOption).require[F](Created) >>= fromTicksSinceEpoch[F]
      val eventData   = (eventType, eventId, contentType).mapN((t, i, ct) => EventData(t, i, data, customMeta, ct))

      (streamId, position, logPosition, eventData, created).mapN((id, p, lp, ed, c) =>
        sec.EventRecord(id, p, lp, ed, c))

    def mkContentType[F[_]](ct: String)(implicit F: ApplicativeThrow[F]): F[ContentType] = ct match
      case ContentTypes.ApplicationOctetStream => F.pure(sec.ContentType.Binary)
      case ContentTypes.ApplicationJson        => F.pure(sec.ContentType.Json)
      case unknown => F.raiseError(ProtoResultError(s"Required value $ContentType missing or invalid: $unknown"))

    def mkEventType[F[_]: ApplicativeThrow](name: String): F[EventType] =
      EventType.stringToEventType(Option(name).getOrElse("")).leftMap(ProtoResultError(_)).liftTo[F]

    def mkWriteResult[F[_]: ApplicativeThrow](sid: StreamId, ar: AppendResp): F[WriteResult] =

      import AppendResp.{Result, Success}
      import AppendResp.WrongExpectedVersion.ExpectedRevisionOption
      import AppendResp.WrongExpectedVersion.CurrentRevisionOption

      def success(s: Result.Success) =

        val logPositionExact: Either[Throwable, LogPosition.Exact] = s.value.positionOption match
          case Success.PositionOption.Position(p)   => LogPosition.exact(p.commitPosition, p.preparePosition).asRight
          case Success.PositionOption.NoPosition(_) => mkError("Did not expect NoPosition when using NonEmptyList")
          case Success.PositionOption.Empty         => mkError("PositionOption is missing")

        val streamPositionExact: Either[Throwable, StreamPosition.Exact] = s.value.currentRevisionOption match
          case Success.CurrentRevisionOption.CurrentRevision(v) => StreamPosition(v).asRight
          case Success.CurrentRevisionOption.NoStream(_) => mkError("Did not expect NoStream when using NonEmptyList")
          case Success.CurrentRevisionOption.Empty       => mkError("CurrentRevisionOption is missing")

        (streamPositionExact, logPositionExact).mapN((r, p) => WriteResult(r, p))

      def wrongExpectedStreamState(w: Result.WrongExpectedVersion) =

        val expected: Either[Throwable, StreamState] = w.value.expectedRevisionOption match
          case ExpectedRevisionOption.ExpectedRevision(v)     => StreamPosition(v).asRight
          case ExpectedRevisionOption.ExpectedNoStream(_)     => StreamState.NoStream.asRight
          case ExpectedRevisionOption.ExpectedAny(_)          => StreamState.Any.asRight
          case ExpectedRevisionOption.ExpectedStreamExists(_) => StreamState.StreamExists.asRight
          case ExpectedRevisionOption.Empty                   => mkError("ExpectedRevisionOption is missing")

        val actual: Either[Throwable, StreamState] = w.value.currentRevisionOption match
          case CurrentRevisionOption.CurrentRevision(v) => StreamPosition(v).asRight
          case CurrentRevisionOption.CurrentNoStream(_) => StreamState.NoStream.asRight
          case CurrentRevisionOption.Empty              => mkError("CurrentRevisionOption is missing")

        (expected, actual).mapN((e, a) => WrongExpectedState(sid, e, a))

      val result: Either[Throwable, WriteResult] = ar.result match
        case s: Result.Success              => success(s)
        case w: Result.WrongExpectedVersion => wrongExpectedStreamState(w) >>= (_.asLeft[WriteResult])
        case Result.Empty                   => mkError("Result is missing")

      result.liftTo[F]

    def mkBatchWriteResult[F[_]](bar: BatchAppendResp)(implicit F: MonadThrow[F]): F[WriteResult] =

      import BatchAppendResp._
      import com.google.rpc._

      def success(s: Success): Either[Throwable, WriteResult] = {

        val logPositionExact: Either[Throwable, LogPosition.Exact] = s.positionOption match {
          case Success.PositionOption.Position(p)   => LogPosition.exact(p.commitPosition, p.preparePosition).asRight
          case Success.PositionOption.NoPosition(_) => mkError("Did not expect NoPosition")
          case Success.PositionOption.Empty         => mkError("PositionOption is missing")
        }

        val streamPositionExact: Either[Throwable, StreamPosition.Exact] = s.currentRevisionOption match {
          case Success.CurrentRevisionOption.CurrentRevision(v) => StreamPosition(v).asRight
          case Success.CurrentRevisionOption.NoStream(_) => mkError("Did not expect NoStream when using NonEmptyList")
          case Success.CurrentRevisionOption.Empty       => mkError("CurrentRevisionOption is missing")
        }

        (streamPositionExact, logPositionExact).mapN((r, p) => WriteResult(r, p))
      }

      def failure(f: Status): F[WriteResult] =

        def raise(t: Throwable): F[WriteResult] =
          F.raiseError[WriteResult](t)

        def mkUnknown(msg: String): UnknownError =
          UnknownError(s"code: ${f.code.name} - message: $msg")

        def handleWrongExpectedVersion(wev: pshared.WrongExpectedVersion): F[WriteResult] =
          mkStreamId[F](bar.streamIdentifier) >>= { mkWrongExpectedStreamState[F](_, wev) >>= raise }

        def handleStreamDeleted(sd: pshared.StreamDeleted): F[WriteResult] =
          mkStreamDeleted[F](sd) >>= raise

        f.details.fold(raise(mkUnknown(f.message))) { v =>
          if (v.is[pshared.WrongExpectedVersion])
            handleWrongExpectedVersion(v.unpack[pshared.WrongExpectedVersion])
          else if (v.is[pshared.StreamDeleted])
            handleStreamDeleted(v.unpack[pshared.StreamDeleted])
          else if (v.is[pshared.AccessDenied])
            raise(AccessDenied)
          else if (v.is[pshared.Timeout])
            raise(new TimeoutException(s"Timeout - code: ${f.code.name} - message: ${f.message}"))
          else if (v.is[pshared.Unknown])
            raise(mkUnknown(f.message))
          else if (v.is[pshared.InvalidTransaction])
            raise(InvalidTransaction)
          else if (v.is[pshared.MaximumAppendSizeExceeded])
            raise(mkMaximumAppendSizeExceeded(v.unpack[pshared.MaximumAppendSizeExceeded]))
          else if (v.is[pshared.BadRequest])
            raise(mkUnknown(v.unpack[pshared.BadRequest].message))
          else
            raise(mkUnknown(f.message))
        }

      bar.result match
        case BatchAppendResp.Result.Success(v)   => success(v).liftTo[F]
        case BatchAppendResp.Result.Error(value) => failure(value)
        case BatchAppendResp.Result.Empty        => mkError("Result is missing").liftTo[F]

    ///

    def mkDeleteResult[F[_]: ApplicativeThrow](dr: DeleteResp): F[DeleteResult] =
      dr.positionOption.position
        .map(p => DeleteResult(LogPosition.exact(p.commitPosition, p.preparePosition)))
        .require[F]("DeleteResp.PositionOptions.Position")

    def mkTombstoneResult[F[_]: ApplicativeThrow](tr: TombstoneResp): F[TombstoneResult] =
      tr.positionOption.position
        .map(p => TombstoneResult(LogPosition.exact(p.commitPosition, p.preparePosition)))
        .require[F]("TombstoneResp.PositionOptions.Position")

    // ====================================================================================================================

    def mkStreamMessageNotFound[F[_]: MonadThrow](p: ReadResp.StreamNotFound): F[StreamMessage.NotFound] =
      p.streamIdentifier.require[F]("StreamIdentifer") >>= { mkStreamId[F](_).map(StreamMessage.NotFound(_)) }

    def mkStreamMessageEvent[F[_]: MonadThrow](p: ReadResp.ReadEvent): F[Option[StreamMessage.StreamEvent]] =
      mkEvent[F](p).map(_.map(StreamMessage.StreamEvent(_)))

    def mkStreamMessageFirst[F[_]: Applicative](v: Long): F[StreamMessage.FirstStreamPosition] =
      StreamMessage.FirstStreamPosition(StreamPosition(v)).pure[F]

    def mkStreamMessageLast[F[_]: Applicative](v: Long): F[StreamMessage.LastStreamPosition] =
      StreamMessage.LastStreamPosition(StreamPosition(v)).pure[F]

    def mkAllMessageEvent[F[_]: MonadThrow](p: ReadResp.ReadEvent): F[Option[AllMessage.AllEvent]] =
      mkEvent[F](p).map(_.map(AllMessage.AllEvent(_)))

    def mkAllMessageLast[F[_]: ApplicativeThrow](p: pshared.AllStreamPosition): F[AllMessage.LastAllStreamPosition] =
      LogPosition(p.commitPosition, p.preparePosition).liftTo[F].map(AllMessage.LastAllStreamPosition(_))

    // ======================================================================================================================

    sealed trait AllResult
    object AllResult:

      import ReadResp.Content as c

      final case class EventR(event: Option[sec.Event]) extends AllResult
      final case class CheckpointR(checkpoint: Checkpoint) extends AllResult
      final case class ConfirmationR(confirmation: SubscriptionConfirmation) extends AllResult
      final case class LastPositionR(position: LogPosition) extends AllResult

      def fromWire[F[_]](p: ReadResp)(implicit F: MonadThrow[F]): F[AllResult] = p.content match
        case c.Event(v)                 => mkEvent[F](v).map(EventR(_))
        case c.Checkpoint(v)            => mkCheckpoint[F](v).map(CheckpointR(_))
        case c.Confirmation(v)          => F.pure(ConfirmationR(SubscriptionConfirmation(v.subscriptionId)))
        case c.LastAllStreamPosition(v) =>
          v.toLogPosition.leftMap(e => ProtoResultError(e.msg)).liftTo[F].map(LastPositionR(_))
        case m => F.raiseError(ProtoResultError(s"Unexpected response for AllResult: $m"))

      //

      extension (ar: AllResult)

        def fold[A](
          eventFn: EventR => A,
          checkpointFn: CheckpointR => A,
          confirmationFn: ConfirmationR => A,
          lastPositionFn: LastPositionR => A
        ): A = ar match {
          case x: EventR        => eventFn(x)
          case x: CheckpointR   => checkpointFn(x)
          case x: ConfirmationR => confirmationFn(x)
          case x: LastPositionR => lastPositionFn(x)
        }

        def toAllMessage: Option[AllMessage] = fold(
          _.event.map(AllMessage.AllEvent(_)),
          _ => none,
          _ => none,
          x => AllMessage.LastAllStreamPosition(x.position).some
        )

    sealed trait StreamResult
    object StreamResult:

      import ReadResp.Content as c

      final case class EventR(event: Option[sec.Event]) extends StreamResult
      final case class CheckpointR(checkpoint: Checkpoint) extends StreamResult
      final case class ConfirmationR(confirmation: SubscriptionConfirmation) extends StreamResult
      final case class StreamNotFoundR(streamId: StreamId) extends StreamResult
      final case class FirstPositionR(position: StreamPosition) extends StreamResult
      final case class LastPositionR(position: StreamPosition) extends StreamResult

      def fromWire[F[_]](p: ReadResp)(implicit F: MonadThrow[F]): F[StreamResult] = p.content match
        case c.Event(v)               => mkEvent[F](v).map(EventR(_))
        case c.Confirmation(v)        => F.pure(ConfirmationR(SubscriptionConfirmation(v.subscriptionId)))
        case c.Checkpoint(v)          => mkCheckpoint[F](v).map(CheckpointR(_))
        case c.StreamNotFound(v)      => mkStreamId[F](v.streamIdentifier).map(StreamNotFoundR(_))
        case c.FirstStreamPosition(v) => F.pure(FirstPositionR(StreamPosition(v)))
        case c.LastStreamPosition(v)  => F.pure(LastPositionR(StreamPosition(v)))
        case m                        => F.raiseError(ProtoResultError(s"Unexpected response: $m"))

      //

      extension (sr: StreamResult)

        def fold[A](
          eventFn: EventR => A,
          checkpointFn: CheckpointR => A,
          confirmationFn: ConfirmationR => A,
          streamNotFoundFn: StreamNotFoundR => A,
          firstPositonFn: FirstPositionR => A,
          lastPositionFn: LastPositionR => A
        ): A = sr match
          case x: EventR          => eventFn(x)
          case x: CheckpointR     => checkpointFn(x)
          case x: ConfirmationR   => confirmationFn(x)
          case x: StreamNotFoundR => streamNotFoundFn(x)
          case x: FirstPositionR  => firstPositonFn(x)
          case x: LastPositionR   => lastPositionFn(x)

        def toStreamMessage: Option[StreamMessage] = fold(
          _.event.map(StreamMessage.StreamEvent(_)),
          _ => none,
          _ => none,
          x => StreamMessage.NotFound(x.streamId).some,
          x => StreamMessage.FirstStreamPosition(x.position).some,
          x => StreamMessage.LastStreamPosition(x.position).some
        )

  end incoming

end streams
