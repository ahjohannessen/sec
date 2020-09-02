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
  ): F[WriteResult] =
    s.appendToStream(streamId, expectedRevision, events, credentials)

  /// Delete

  def softDelete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    credentials: Option[UserCredentials] = None
  ): F[DeleteResult] =
    s.softDelete(streamId, expectedRevision, credentials)

  def hardDelete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    credentials: Option[UserCredentials] = None
  ): F[DeleteResult] =
    s.hardDelete(streamId, expectedRevision, credentials)

}
