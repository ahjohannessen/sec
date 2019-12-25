package sec
package syntax

import cats.data.NonEmptyList
import fs2.Stream
import sec.core.{Event, EventData, EventFilter, EventNumber, Position, StreamId, StreamRevision}
import sec.api._

final class StreamsSyntax[F[_]](val s: Streams[F]) extends AnyVal {

  /// Subscription

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean = false,
    filter: Option[EventFilter] = None,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.subscribeToAll(exclusiveFrom, resolveLinkTos, filter, credentials)

  def subscribeToStream(
    streamId: StreamId,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean = false,
    failIfNotFound: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.subscribeToStream(streamId, exclusiveFrom, resolveLinkTos, failIfNotFound, credentials)

  /// Read

  def readStreamForwards(
    streamId: StreamId,
    from: EventNumber,
    count: Int,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.readStream(streamId, from, ReadDirection.Forward, count, resolveLinkTos, credentials)

  def readStreamBackwards(
    streamId: StreamId,
    from: EventNumber,
    count: Int,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.readStream(streamId, from, ReadDirection.Backward, count, resolveLinkTos, credentials)

  def readAllForwards(
    position: Position,
    maxCount: Int,
    resolveLinkTos: Boolean = false,
    filter: Option[EventFilter] = None,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.readAll(position, ReadDirection.Forward, maxCount, resolveLinkTos, filter, credentials)

  def readAllBackwards(
    position: Position,
    maxCount: Int,
    resolveLinkTos: Boolean = false,
    filter: Option[EventFilter] = None,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.readAll(position, ReadDirection.Backward, maxCount, resolveLinkTos, filter, credentials)

  /// Append

  def appendToStream(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    events: NonEmptyList[EventData],
    credentials: Option[UserCredentials] = None
  ): F[Streams.WriteResult] =
    s.appendToStream(streamId, expectedRevision, events, credentials)

  /// Delete

  def softDelete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    credentials: Option[UserCredentials] = None
  ): F[Streams.DeleteResult] =
    s.softDelete(streamId, expectedRevision, credentials)

  def hardDelete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    credentials: Option[UserCredentials] = None
  ): F[Streams.DeleteResult] =
    s.hardDelete(streamId, expectedRevision, credentials)

}
