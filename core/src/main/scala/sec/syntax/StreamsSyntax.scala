package sec
package syntax

import cats.data.NonEmptyList
import fs2.Stream
import sec.core._

final class StreamsSyntax[F[_]](val esc: Streams[F]) extends AnyVal {

  /// Subscription

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean = false,
    filter: Option[EventFilter] = None,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    esc.subscribeToAll(exclusiveFrom, resolveLinkTos, filter, credentials)

  def subscribeToStream(
    stream: String,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    esc.subscribeToStream(stream, exclusiveFrom, resolveLinkTos, credentials)

  /// Read

  def readStreamForwards(
    stream: String,
    from: EventNumber,
    count: Int,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    esc.readStream(stream, ReadDirection.Forward, from, count, resolveLinkTos, credentials)

  def readStreamBackwards(
    stream: String,
    from: EventNumber,
    count: Int,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    esc.readStream(stream, ReadDirection.Backward, from, count, resolveLinkTos, credentials)

  def readAllForwards(
    position: Position,
    maxCount: Int,
    resolveLinkTos: Boolean = false,
    filter: Option[EventFilter] = None,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    esc.readAll(position, ReadDirection.Forward, maxCount, resolveLinkTos, filter, credentials)

  def readAllBackwards(
    position: Position,
    maxCount: Int,
    resolveLinkTos: Boolean = false,
    filter: Option[EventFilter] = None,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    esc.readAll(position, ReadDirection.Backward, maxCount, resolveLinkTos, filter, credentials)

  /// Append

  def appendToStream(
    stream: String,
    expectedRevision: StreamRevision,
    events: NonEmptyList[EventData],
    credentials: Option[UserCredentials] = None
  ): F[Streams.WriteResult] = esc.appendToStream(stream, expectedRevision, events, credentials)

  /// Delete

  def softDelete(
    stream: String,
    expectedRevision: StreamRevision,
    credentials: Option[UserCredentials] = None
  ): F[Streams.DeleteResult] = esc.softDelete(stream, expectedRevision, credentials)

  def hardDelete(
    stream: String,
    expectedRevision: StreamRevision,
    credentials: Option[UserCredentials] = None
  ): F[Streams.DeleteResult] = esc.hardDelete(stream, expectedRevision, credentials)

}
