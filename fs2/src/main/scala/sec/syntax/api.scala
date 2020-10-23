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

import fs2.Stream
import sec.api._

//======================================================================================================================

trait ApiSyntax extends StreamsSyntax with MetaStreamsSyntax

//======================================================================================================================

trait StreamsSyntax {
  implicit final def syntaxForStreams[F[_]](ms: Streams[F]): StreamsOps[F] = new StreamsOps[F](ms)
}

final class StreamsOps[F[_]](val s: Streams[F]) extends AnyVal {

  /// Subscription

  def subscribeToAll(
    exclusiveFrom: Option[LogPosition]
  ): Stream[F, Event] =
    s.subscribeToAll(exclusiveFrom, resolveLinkTos = false)

  def subscribeToAllFiltered(
    exclusiveFrom: Option[LogPosition],
    filterOptions: SubscriptionFilterOptions
  ): Stream[F, Either[Checkpoint, Event]] =
    s.subscribeToAll(exclusiveFrom, filterOptions, resolveLinkTos = false)

  def subscribeToStream(
    streamId: StreamId,
    exclusiveFrom: Option[StreamPosition]
  ): Stream[F, Event] =
    s.subscribeToStream(streamId, exclusiveFrom, resolveLinkTos = false)

  /// Read

  def readStreamForwards(
    streamId: StreamId,
    from: StreamPosition = StreamPosition.Start,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, Event] =
    s.readStream(streamId, from, Direction.Forwards, maxCount, resolveLinkTos)

  def readStreamBackwards(
    streamId: StreamId,
    from: StreamPosition = StreamPosition.End,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, Event] =
    s.readStream(streamId, from, Direction.Backwards, maxCount, resolveLinkTos)

  def readAllForwards(
    from: LogPosition = LogPosition.Start,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, Event] =
    s.readAll(from, Direction.Forwards, maxCount, resolveLinkTos)

  def readAllBackwards(
    from: LogPosition = LogPosition.End,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, Event] =
    s.readAll(from, Direction.Backwards, maxCount, resolveLinkTos)

}

//======================================================================================================================
