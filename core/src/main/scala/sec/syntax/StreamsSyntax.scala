package sec
package syntax

import cats.data.NonEmptyList
import fs2.Stream
import sec.core._
import com.eventstore.client.streams.{DeleteResp, ReadResp, TombstoneResp} // temp

final class StreamsSyntax[F[_]](val esc: Streams[F]) extends AnyVal {

  /// Subscription

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean = false,
    filter: Option[EventFilter] = None,
    credentials: Option[UserCredentials] = None
  ): Stream[F, ReadResp] =
    esc.subscribeToAll(exclusiveFrom, resolveLinkTos, filter, credentials)

  def subscribeToStream(
    stream: String,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, ReadResp] =
    esc.subscribeToStream(stream, exclusiveFrom, resolveLinkTos, credentials)

  /// Read

  def readStreamForwards(
    stream: String,
    from: EventNumber,
    count: Int,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, ReadResp] =
    esc.readStream(stream, ReadDirection.Forward, from, count, resolveLinkTos, credentials)

  def readStreamBackwards(
    stream: String,
    from: EventNumber,
    count: Int,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, ReadResp] =
    esc.readStream(stream, ReadDirection.Backward, from, count, resolveLinkTos, credentials)

  def readAllForwards(
    position: Position,
    maxCount: Int,
    resolveLinkTos: Boolean = false,
    filter: Option[EventFilter] = None,
    credentials: Option[UserCredentials] = None
  ): Stream[F, ReadResp] =
    esc.readAll(position, ReadDirection.Forward, maxCount, resolveLinkTos, filter, credentials)

  def readAllBackwards(
    position: Position,
    maxCount: Int,
    resolveLinkTos: Boolean = false,
    filter: Option[EventFilter] = None,
    credentials: Option[UserCredentials] = None
  ): Stream[F, ReadResp] =
    esc.readAll(position, ReadDirection.Backward, maxCount, resolveLinkTos, filter, credentials)

  /// Append

  def appendToStream(
    stream: String,
    expectedRevision: StreamRevision,
    events: NonEmptyList[EventData],
    credentials: Option[UserCredentials] = None
  ): F[WriteResult] = esc.appendToStream(stream, expectedRevision, events, credentials)

  /// Delete

  def softDelete(
    stream: String,
    expectedRevision: StreamRevision,
    credentials: Option[UserCredentials] = None
  ): F[DeleteResp] = esc.softDelete(stream, expectedRevision, credentials)

  def tombstone(
    stream: String,
    expectedRevision: StreamRevision,
    credentials: Option[UserCredentials] = None
  ): F[TombstoneResp] = esc.tombstone(stream, expectedRevision, credentials)

}
