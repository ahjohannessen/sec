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

import cats.data.NonEmptyList

trait Streams {

  type Task[+A]   = zio.Task[A]
  type Stream[+A] = zio.stream.Stream[Throwable, A]

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[Event]

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    filterOptions: SubscriptionFilterOptions,
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[Either[Checkpoint, Event]]

  def subscribeToStream(
    streamId: StreamId,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[Event]

  def readAll(
    from: Position,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[Event]

  def readStream(
    streamId: StreamId,
    from: EventNumber,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[Event]

  def appendToStream(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    events: NonEmptyList[EventData],
    creds: Option[UserCredentials]
  ): Task[WriteResult]

  def delete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): Task[DeleteResult]

  def tombstone(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): Task[DeleteResult]

}
