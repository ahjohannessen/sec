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
import sec.api.*

trait StreamsSyntax:

  extension [F[_]](s: Streams[F])

    // / Subscription

    /** Subscribes to the global stream, [[StreamId.All]] without resolving [[EventType.LinkTo]] events.
      *
      * @param exclusiveFrom
      *   position to start from. Use [[scala.None]] to subscribe from the beginning.
      * @return
      *   a [[fs2.Stream]] that emits [[Event]] values.
      */
    def subscribeToAll(
      exclusiveFrom: Option[LogPosition]
    ): Stream[F, Event] =
      s.subscribeToAll(exclusiveFrom, resolveLinkTos = false)

    /** Subscribes to the global stream, [[StreamId.All]] using a subscription filter without resolving
      * [[EventType.LinkTo]] events.
      *
      * @param exclusiveFrom
      *   log position to start from. Use [[scala.None]] to subscribe from the beginning.
      * @param filterOptions
      *   to use when subscribing - See [[sec.api.SubscriptionFilterOptions]].
      * @return
      *   a [[fs2.Stream]] that emits either [[sec.api.Checkpoint]] or [[Event]] values. How frequent
      *   [[sec.api.Checkpoint]] is emitted depends on @filterOptions.
      */
    def subscribeToAll(
      exclusiveFrom: Option[LogPosition],
      filterOptions: SubscriptionFilterOptions
    ): Stream[F, Either[Checkpoint, Event]] =
      s.subscribeToAll(exclusiveFrom, filterOptions, resolveLinkTos = false)

    /** Subscribes to an individual stream without resolving [[EventType.LinkTo]] events.
      *
      * @param streamId
      *   the id of the stream to subscribe to.
      * @param exclusiveFrom
      *   stream position to start from. Use [[scala.None]] to subscribe from the beginning.
      * @return
      *   a [[fs2.Stream]] that emits [[Event]] values.
      */
    def subscribeToStream(
      streamId: StreamId,
      exclusiveFrom: Option[StreamPosition]
    ): Stream[F, Event] =
      s.subscribeToStream(streamId, exclusiveFrom, resolveLinkTos = false)

    // / Read

    /** Read events forwards from the global stream, [[sec.StreamId.All]].
      *
      * @param from
      *   log position to read from.
      * @param maxCount
      *   limits maximum events returned.
      * @param resolveLinkTos
      *   whether to resolve [[EventType.LinkTo]] events automatically.
      * @return
      *   a [[fs2.Stream]] that emits [[Event]] values.
      */
    def readAllForwards(
      from: LogPosition = LogPosition.Start,
      maxCount: Long = Long.MaxValue,
      resolveLinkTos: Boolean = false
    ): Stream[F, Event] =
      s.readAll(from, Direction.Forwards, maxCount, resolveLinkTos)

    /** Read events backwards from the global stream, [[sec.StreamId.All]].
      *
      * @param from
      *   log position to read from.
      * @param maxCount
      *   limits maximum events returned.
      * @param resolveLinkTos
      *   whether to resolve [[EventType.LinkTo]] events automatically.
      * @return
      *   a [[fs2.Stream]] that emits [[Event]] values.
      */
    def readAllBackwards(
      from: LogPosition = LogPosition.End,
      maxCount: Long = Long.MaxValue,
      resolveLinkTos: Boolean = false
    ): Stream[F, Event] =
      s.readAll(from, Direction.Backwards, maxCount, resolveLinkTos)

    /** Read events forwards from an individual stream. A [[sec.api.exceptions.StreamNotFound]] is raised when the
      * stream does not exist.
      *
      * @param streamId
      *   the id of the stream to read from.
      * @param from
      *   stream position to read from.
      * @param maxCount
      *   limits maximum events returned.
      * @param resolveLinkTos
      *   whether to resolve [[EventType.LinkTo]] events automatically.
      * @return
      *   a [[fs2.Stream]] that emits [[Event]] values.
      */
    def readStreamForwards(
      streamId: StreamId,
      from: StreamPosition = StreamPosition.Start,
      maxCount: Long = Long.MaxValue,
      resolveLinkTos: Boolean = false
    ): Stream[F, Event] =
      s.readStream(streamId, from, Direction.Forwards, maxCount, resolveLinkTos)

    /** Read events backwards from an individual stream. A [[sec.api.exceptions.StreamNotFound]] is raised when the
      * stream does not exist.
      *
      * @param streamId
      *   the id of the stream to read from.
      * @param from
      *   stream position to read from.
      * @param maxCount
      *   limits maximum events returned.
      * @param resolveLinkTos
      *   whether to resolve [[EventType.LinkTo]] events automatically.
      * @return
      *   a [[fs2.Stream]] that emits [[Event]] values.
      */
    def readStreamBackwards(
      streamId: StreamId,
      from: StreamPosition = StreamPosition.End,
      maxCount: Long = Long.MaxValue,
      resolveLinkTos: Boolean = false
    ): Stream[F, Event] =
      s.readStream(streamId, from, Direction.Backwards, maxCount, resolveLinkTos)
