package sec
package syntax

import cats.data.NonEmptyList
import fs2.Stream
import sec.core._
import sec.api._

final class StreamsSyntax[F[_]](val s: Streams[F]) extends AnyVal {

  /// Subscription

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.subscribeToAll(exclusiveFrom, resolveLinkTos, credentials)

  def subscribeToAllFiltered(
    exclusiveFrom: Option[Position],
    filter: EventFilter,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Either[Position, Event]] =
    s.subscribeToAll(exclusiveFrom, filter, resolveLinkTos, credentials)

  def subscribeToStream(
    streamId: StreamId,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.subscribeToStream(streamId, exclusiveFrom, resolveLinkTos, credentials)

  /// Read

  def readStreamForwards(
    streamId: StreamId,
    from: EventNumber,
    maxCount: Long,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.readStream(streamId, from, Direction.Forwards, maxCount, resolveLinkTos, credentials)

  def readStreamBackwards(
    streamId: StreamId,
    from: EventNumber,
    maxCount: Long,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.readStream(streamId, from, Direction.Backwards, maxCount, resolveLinkTos, credentials)

  def readAllForwards(
    position: Position,
    maxCount: Long,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.readAll(position, Direction.Forwards, maxCount, resolveLinkTos, credentials)

  def readAllBackwards(
    position: Position,
    maxCount: Long,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.readAll(position, Direction.Backwards, maxCount, resolveLinkTos, credentials)

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
