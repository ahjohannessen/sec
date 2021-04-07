/*
 * Copyright 2020 Scala EventStoreDB Client
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

trait StreamsSyntax {
  implicit final def syntaxForStreams[F[_]](ms: Streams[F]): StreamsOps[F] = new StreamsOps[F](ms)
}

final class StreamsOps[F[_]](val s: Streams[F]) extends AnyVal {

  /// Subscription

  /**
   * Subscribes to the global stream, [[StreamId.All]] without
   * resolving [[EventType.LinkTo]] events.
   *
   * @param exclusiveFrom position to start from. Use [[None]] to subscribe from the beginning.
   * @return a [[Stream]] that emits [[AllEvent]] values.
   */
  def subscribeToAll(
    exclusiveFrom: Option[LogPosition]
  ): Stream[F, AllEvent] =
    s.subscribeToAll(exclusiveFrom, resolveLinkTos = false)

  /**
   * Subscribes to the global stream, [[StreamId.All]] using a subscription filter without
   * resolving [[EventType.LinkTo]] events.
   *
   * @param exclusiveFrom log position to start from. Use [[None]] to subscribe from the beginning.
   * @param filterOptions to use when subscribing - See [[sec.api.SubscriptionFilterOptions]].
   * @return a [[Stream]] that emits either [[Checkpoint]] or [[AllEvent]] values.
   *         How frequent [[Checkpoint]] is emitted depends on [[filterOptions]].
   */
  def subscribeToAll(
    exclusiveFrom: Option[LogPosition],
    filterOptions: SubscriptionFilterOptions
  ): Stream[F, Either[Checkpoint, AllEvent]] =
    s.subscribeToAll(exclusiveFrom, filterOptions, resolveLinkTos = false)

  /**
   * Subscribes to an individual stream without resolving [[EventType.LinkTo]] events.
   *
   * @param streamId the id of the stream to subscribe to.
   * @param exclusiveFrom stream position to start from. Use [[None]] to subscribe from the beginning.
   * @return a [[Stream]] that emits [[StreamEvent]] values.
   */
  def subscribeToStream(
    streamId: StreamId,
    exclusiveFrom: Option[StreamPosition]
  ): Stream[F, StreamEvent] =
    s.subscribeToStream(streamId, exclusiveFrom, resolveLinkTos = false)

  /// Read

  /**
   * Read events forwards from the global stream, [[sec.StreamId.All]].
   *
   * @param from log position to read from.
   * @param maxCount limits maximum events returned.
   * @param resolveLinkTos whether to resolve [[EventType.LinkTo]] events automatically.
   * @return a [[Stream]] that emits [[AllEvent]] values.
   */
  def readAllForwards(
    from: LogPosition = LogPosition.Start,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, AllEvent] =
    s.readAll(from, Direction.Forwards, maxCount, resolveLinkTos)

  /**
   * Read events backwards from the global stream, [[sec.StreamId.All]].
   *
   * @param from log position to read from.
   * @param maxCount limits maximum events returned.
   * @param resolveLinkTos whether to resolve [[EventType.LinkTo]] events automatically.
   * @return a [[Stream]] that emits [[AllEvent]] values.
   */
  def readAllBackwards(
    from: LogPosition = LogPosition.End,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, AllEvent] =
    s.readAll(from, Direction.Backwards, maxCount, resolveLinkTos)

  /**
   * Read events forwards from an individual stream. A [[sec.api.exceptions.StreamNotFound]] is raised
   * when the stream does not exist.
   *
   * @param streamId the id of the stream to read from.
   * @param from stream position to read from.
   * @param maxCount limits maximum events returned.
   * @param resolveLinkTos whether to resolve [[EventType.LinkTo]] events automatically.
   * @return a [[Stream]] that emits [[StreamEvent]] values.
   */
  def readStreamForwards(
    streamId: StreamId,
    from: StreamPosition = StreamPosition.Start,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, StreamEvent] =
    s.readStream(streamId, from, Direction.Forwards, maxCount, resolveLinkTos)

  /**
   * Read events backwards from an individual stream. A [[sec.api.exceptions.StreamNotFound]] is raised
   * when the stream does not exist.
   *
   * @param streamId the id of the stream to read from.
   * @param from stream position to read from.
   * @param maxCount limits maximum events returned.
   * @param resolveLinkTos whether to resolve [[EventType.LinkTo]] events automatically.
   * @return a [[Stream]] that emits [[StreamEvent]] values.
   */
  def readStreamBackwards(
    streamId: StreamId,
    from: StreamPosition = StreamPosition.End,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, StreamEvent] =
    s.readStream(streamId, from, Direction.Backwards, maxCount, resolveLinkTos)

}
