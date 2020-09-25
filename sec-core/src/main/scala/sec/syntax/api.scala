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

//======================================================================================================================

trait ApiSyntax extends StreamsSyntax with MetaStreamsSyntax

//======================================================================================================================

trait StreamsSyntax {
  implicit final def syntaxForStreams[F[_]](ms: Streams[F]): StreamsOps[F] = new StreamsOps[F](ms)
}

final class StreamsOps[F[_]](val s: Streams[F]) extends AnyVal {

  import sec.api.Streams.{Checkpoint, DeleteResult, WriteResult}

  /// Subscription

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.subscribeToAll(exclusiveFrom, resolveLinkTos, credentials)

  def subscribeToAllFiltered(
    exclusiveFrom: Option[Position],
    filterOptions: SubscriptionFilterOptions,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Either[Checkpoint, Event]] =
    s.subscribeToAll(exclusiveFrom, filterOptions, resolveLinkTos, credentials)

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
    from: EventNumber = EventNumber.Start,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.readStream(streamId, from, Direction.Forwards, maxCount, resolveLinkTos, credentials)

  def readStreamBackwards(
    streamId: StreamId,
    from: EventNumber = EventNumber.End,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.readStream(streamId, from, Direction.Backwards, maxCount, resolveLinkTos, credentials)

  def readAllForwards(
    position: Position = Position.Start,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false,
    credentials: Option[UserCredentials] = None
  ): Stream[F, Event] =
    s.readAll(position, Direction.Forwards, maxCount, resolveLinkTos, credentials)

  def readAllBackwards(
    position: Position = Position.End,
    maxCount: Long = Long.MaxValue,
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

  def delete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    credentials: Option[UserCredentials] = None
  ): F[DeleteResult] =
    s.delete(streamId, expectedRevision, credentials)

  def tombstone(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    credentials: Option[UserCredentials] = None
  ): F[DeleteResult] =
    s.tombstone(streamId, expectedRevision, credentials)

}

//======================================================================================================================
