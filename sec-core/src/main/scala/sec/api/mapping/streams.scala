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

import cats.data.NonEmptyList
import cats.syntax.all._
import com.eventstore.client._
import com.eventstore.client.streams._
import sec.core._
import sec.api.exceptions.WrongExpectedVersion
import sec.api.Streams._
import sec.api.mapping.implicits._
import sec.api.mapping.time._
import sec.api.mapping.shared._
import sec.api.grpc.constants.Metadata.{ContentType, ContentTypes, Created, Type}

private[sec] object streams {

//======================================================================================================================
//                                                     Outgoing
//======================================================================================================================

  object outgoing {

    val empty: Empty = Empty()

    val uuidOption: ReadReq.Options.UUIDOption = ReadReq.Options.UUIDOption().withStructured(empty)

    val mapPosition: Position => ReadReq.Options.AllOptions.AllOption = {
      case Position.Exact(c, p) => ReadReq.Options.AllOptions.AllOption.Position(ReadReq.Options.Position(c, p))
      case Position.End         => ReadReq.Options.AllOptions.AllOption.End(empty)
    }

    val mapPositionOpt: Option[Position] => ReadReq.Options.AllOptions.AllOption = {
      case Some(v) => mapPosition(v)
      case None    => ReadReq.Options.AllOptions.AllOption.Start(empty)
    }

    val mapEventNumber: EventNumber => ReadReq.Options.StreamOptions.RevisionOption = {
      case EventNumber.Exact(nr) => ReadReq.Options.StreamOptions.RevisionOption.Revision(nr)
      case EventNumber.End       => ReadReq.Options.StreamOptions.RevisionOption.End(empty)
    }

    val mapEventNumberOpt: Option[EventNumber] => ReadReq.Options.StreamOptions.RevisionOption = {
      case Some(v) => mapEventNumber(v)
      case None    => ReadReq.Options.StreamOptions.RevisionOption.Start(empty)
    }

    val mapDirection: Direction => ReadReq.Options.ReadDirection = {
      case Direction.Forwards  => ReadReq.Options.ReadDirection.Forwards
      case Direction.Backwards => ReadReq.Options.ReadDirection.Backwards
    }

    val mapReadEventFilter: Option[EventFilter] => ReadReq.Options.FilterOption = {

      import ReadReq.Options.FilterOption

      def filter(filter: EventFilter): FilterOption = {

        val expr = filter.option.fold(
          nel => ReadReq.Options.FilterOptions.Expression().withPrefix(nel.map(_.value).toList),
          reg => ReadReq.Options.FilterOptions.Expression().withRegex(reg.value)
        )

        val window = filter.maxSearchWindow
          .map(ReadReq.Options.FilterOptions.Window.Max)
          .getOrElse(ReadReq.Options.FilterOptions.Window.Count(empty))

        val result = filter.kind match {
          case EventFilter.ByStreamId  => ReadReq.Options.FilterOptions().withStreamName(expr).withWindow(window)
          case EventFilter.ByEventType => ReadReq.Options.FilterOptions().withEventType(expr).withWindow(window)
        }

        FilterOption.Filter(result)
      }

      def noFilter: FilterOption = FilterOption.NoFilter(empty)

      _.fold(noFilter)(filter)
    }

    def mkSubscribeToStreamReq(
      streamId: StreamId,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean
    ): ReadReq = {

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(streamId.esSid.some, mapEventNumberOpt(exclusiveFrom)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(Direction.Forwards))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(empty)
        .withUuidOption(uuidOption)

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
        .withReadDirection(mapDirection(Direction.Forwards))
        .withResolveLinks(resolveLinkTos)
        .withFilterOption(mapReadEventFilter(filter))
        .withUuidOption(uuidOption)

      ReadReq().withOptions(options)
    }

    def mkReadStreamReq(
      streamId: StreamId,
      from: EventNumber,
      direction: Direction,
      count: Long,
      resolveLinkTos: Boolean
    ): ReadReq = {

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(streamId.esSid.some, mapEventNumber(from)))
        .withCount(count)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(empty)
        .withUuidOption(uuidOption)

      ReadReq().withOptions(options)
    }

    def mkReadAllReq(
      position: Position,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean
    ): ReadReq = {

      val options = ReadReq
        .Options()
        .withAll(ReadReq.Options.AllOptions(mapPosition(position)))
        .withCount(maxCount)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(empty)
        .withUuidOption(uuidOption)

      ReadReq().withOptions(options)
    }

    def mkDeleteReq(streamId: StreamId, expectedRevision: StreamRevision): DeleteReq = {

      val mapDeleteRevision: StreamRevision => DeleteReq.Options.ExpectedStreamRevision = {
        case EventNumber.Exact(v)        => DeleteReq.Options.ExpectedStreamRevision.Revision(v)
        case StreamRevision.NoStream     => DeleteReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamRevision.StreamExists => DeleteReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamRevision.Any          => DeleteReq.Options.ExpectedStreamRevision.Any(empty)
      }
      DeleteReq().withOptions(DeleteReq.Options(streamId.esSid.some, mapDeleteRevision(expectedRevision)))
    }

    def mkTombstoneReq(streamId: StreamId, expectedRevision: StreamRevision): TombstoneReq = {

      val mapTombstoneRevision: StreamRevision => TombstoneReq.Options.ExpectedStreamRevision = {
        case EventNumber.Exact(v)        => TombstoneReq.Options.ExpectedStreamRevision.Revision(v)
        case StreamRevision.NoStream     => TombstoneReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamRevision.StreamExists => TombstoneReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamRevision.Any          => TombstoneReq.Options.ExpectedStreamRevision.Any(empty)
      }
      TombstoneReq().withOptions(TombstoneReq.Options(streamId.esSid.some, mapTombstoneRevision(expectedRevision)))
    }

    def mkAppendHeaderReq(streamId: StreamId, expectedRevision: StreamRevision): AppendReq = {

      val mapAppendRevision: StreamRevision => AppendReq.Options.ExpectedStreamRevision = {
        case EventNumber.Exact(v)        => AppendReq.Options.ExpectedStreamRevision.Revision(v)
        case StreamRevision.NoStream     => AppendReq.Options.ExpectedStreamRevision.NoStream(empty)
        case StreamRevision.StreamExists => AppendReq.Options.ExpectedStreamRevision.StreamExists(empty)
        case StreamRevision.Any          => AppendReq.Options.ExpectedStreamRevision.Any(empty)
      }
      AppendReq().withOptions(AppendReq.Options(streamId.esSid.some, mapAppendRevision(expectedRevision)))
    }

    def mkAppendProposalsReq(events: NonEmptyList[EventData]): NonEmptyList[AppendReq] =
      events.map { e =>
        val id         = mkUuid(e.eventId)
        val customMeta = e.metadata.bytes.toByteString
        val data       = e.data.bytes.toByteString
        val ct         = e.contentType.fold(ContentTypes.ApplicationOctetStream, ContentTypes.ApplicationJson)
        val meta       = Map(Type -> EventType.eventTypeToString(e.eventType), ContentType -> ct)
        AppendReq().withProposedMessage(AppendReq.ProposedMessage(id.some, meta, customMeta, data))
      }
  }

//======================================================================================================================
//                                                     Incoming
//======================================================================================================================

  object incoming {

    def mkEvent[F[_]: ErrorM](re: ReadResp.ReadEvent): F[Option[Event]] =
      re.event.traverse(mkEventRecord[F]) >>= { eOpt =>
        re.link.traverse(mkEventRecord[F]).map(lOpt => eOpt.map(er => lOpt.fold[Event](er)(ResolvedEvent(er, _))))
      }

    def mkEventRecord[F[_]: ErrorM](e: ReadResp.ReadEvent.RecordedEvent): F[EventRecord] = {

      import EventData.{binary, json}

      val streamId    = mkStreamId[F](e.streamIdentifier)
      val eventNumber = EventNumber.exact(e.streamRevision)
      val position    = Position.exact(e.commitPosition, e.preparePosition)
      val data        = e.data.toByteVector
      val customMeta  = e.customMetadata.toByteVector
      val eventId     = e.id.require[F]("UUID") >>= mkJuuid[F]
      val eventType   = e.metadata.get(Type).require[F](Type) >>= mkEventType[F]
      val contentType = e.metadata.get(ContentType).require[F](ContentType) >>= mkContentType[F]
      val created     = e.metadata.get(Created).flatMap(_.toLongOption).require[F](Created) >>= fromTicksSinceEpoch[F]

      val eventData = (eventType, eventId, contentType).mapN { (t, i, ct) =>
        ct.fold(binary(t, i, data, customMeta), json(t, i, data, customMeta))
      }

      (streamId, eventData, created).mapN((id, ed, c) => EventRecord(id, eventNumber, position, ed, c))

    }

    def mkContentType[F[_]](ct: String)(implicit F: ErrorA[F]): F[Content.Type] =
      ct match {
        case ContentTypes.ApplicationOctetStream => F.pure(Content.Type.Binary)
        case ContentTypes.ApplicationJson        => F.pure(Content.Type.Json)
        case unknown                             => F.raiseError(ProtoResultError(s"Required value $ContentType missing or invalid: $unknown"))
      }

    def mkEventType[F[_]: ErrorA](name: String): F[EventType] =
      EventType.stringToEventType(Option(name).getOrElse("")).leftMap(ProtoResultError).liftTo[F]

    def mkWriteResult[F[_]: ErrorA](sid: StreamId, ar: AppendResp): F[WriteResult] = {

      import com.eventstore.client.streams.AppendResp.{Result, Success}

      def error[T](msg: String): Either[Throwable, T] =
        ProtoResultError(msg).asLeft[T]

      def success(s: Result.Success) = {

        val position: Either[Throwable, Position.Exact] = s.value.positionOption match {
          case Success.PositionOption.Position(p)   => Position.exact(p.commitPosition, p.preparePosition).asRight
          case Success.PositionOption.NoPosition(_) => error("Did not expect NoPosition when using NonEmptyList")
          case Success.PositionOption.Empty         => error("PositionOption is missing")
        }

        val revision: Either[Throwable, EventNumber.Exact] = s.value.currentRevisionOption match {
          case Success.CurrentRevisionOption.CurrentRevision(v) => EventNumber.exact(v).asRight
          case Success.CurrentRevisionOption.NoStream(_)        => error("Did not expect NoStream when using NonEmptyList")
          case Success.CurrentRevisionOption.Empty              => error("CurrentRevisionOptions is missing")
        }

        (revision, position).mapN((r, p) => WriteResult(r, p))

      }

      def wrongExpectedVersion(w: Result.WrongExpectedVersion) = {
        // TODO: Decide what to do with other cases
        val expected = w.value.expectedRevisionOption.expectedRevision
        val actual   = w.value.currentRevisionOption.currentRevision
        WrongExpectedVersion(sid.stringValue, expected, actual).asLeft
      }

      val result = ar.result match {
        case s: Result.Success              => success(s)
        case w: Result.WrongExpectedVersion => wrongExpectedVersion(w)
        case Result.Empty                   => error("Result is missing")
      }

      result.liftTo[F]
    }

    def mkDeleteResult[F[_]: ErrorA](dr: DeleteResp): F[DeleteResult] =
      dr.positionOption.position
        .map(p => DeleteResult(Position.exact(p.commitPosition, p.preparePosition)))
        .require[F]("DeleteResp.PositionOptions.Position")

    def mkDeleteResult[F[_]: ErrorA](tr: TombstoneResp): F[DeleteResult] =
      tr.positionOption.position
        .map(p => DeleteResult(Position.exact(p.commitPosition, p.preparePosition)))
        .require[F]("TombstoneResp.PositionOptions.Position")

  }

}
