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
package api

import cats.Order
import cats.syntax.all.*
import exceptions.StreamNotFound

/** Checkpoint result used with server-side filtering in EventStoreDB. Contains the [[LogPosition.Exact]] when the
  * checkpoint was made.
  */
final case class Checkpoint(
  logPosition: LogPosition.Exact
)

object Checkpoint:

  val endOfStream: Checkpoint =
    Checkpoint(LogPosition.Exact.MaxValue)

  extension (c: Checkpoint)
    def isEndOfStream: Boolean =
      c === endOfStream

  given Order[Checkpoint] = Order.by(_.logPosition)

/** The current last [[StreamPosition.Exact]] of the stream appended to and its corresponding [[LogPosition.Exact]] in
  * the transaction log.
  */
final case class WriteResult(
  streamPosition: StreamPosition.Exact,
  logPosition: LogPosition.Exact
)

/** The [[LogPosition.Exact]] of the delete in the transaction log.
  */
final case class DeleteResult(
  logPosition: LogPosition.Exact
)

/** The [[LogPosition.Exact]] of the tombstone in the transaction log.
  */
final case class TombstoneResult(
  logPosition: LogPosition.Exact
)

/** A subscription confirmation identifier as the first value from the EventStoreDB when subscribing to a stream. Not
  * intended for public use.
  */
final private[sec] case class SubscriptionConfirmation(
  id: String
)

// ====================================================================================================================

/** [[StreamMessage]] represents different kind of messages that you get when reading from a stream.
  *
  * There are four variants:
  *
  *   - [[StreamMessage.StreamEvent]] A regular [[Event]].
  *   - [[StreamMessage.FirstStreamPosition]] The first position of a stream.
  *   - [[StreamMessage.LastStreamPosition]] The last position of a stream.
  *   - [[StreamMessage.NotFound]] Representing a stream that was not found.
  */
sealed trait StreamMessage
object StreamMessage:

  final case class StreamEvent(event: Event) extends StreamMessage
  final case class FirstStreamPosition(position: StreamPosition) extends StreamMessage
  final case class LastStreamPosition(position: StreamPosition) extends StreamMessage
  final case class NotFound(streamId: StreamId) extends StreamMessage
  object NotFound:
    extension (nf: NotFound) def toException: StreamNotFound = StreamNotFound(nf.streamId.render)

  //

  extension (sm: StreamMessage)

    def fold[A](
      seFn: StreamEvent => A,
      fpFn: FirstStreamPosition => A,
      lpFn: LastStreamPosition => A,
      nfFn: NotFound => A
    ): A = sm match
      case x: StreamEvent         => seFn(x)
      case x: FirstStreamPosition => fpFn(x)
      case x: LastStreamPosition  => lpFn(x)
      case x: NotFound            => nfFn(x)

    def event: Option[StreamEvent]         = fold(_.some, _ => none, _ => none, _ => none)
    def first: Option[FirstStreamPosition] = fold(_ => none, _.some, _ => none, _ => none)
    def last: Option[LastStreamPosition]   = fold(_ => none, _ => none, _.some, _ => none)
    def notFound: Option[NotFound]         = fold(_ => none, _ => none, _ => none, _.some)

    def isEvent: Boolean    = event.isDefined
    def isFirst: Boolean    = first.isDefined
    def isLast: Boolean     = last.isDefined
    def isNotFound: Boolean = notFound.isDefined

/** [[AllMessage]] represents different kind of messages that you get when reading from the global stream.
  *
  * There are two variants:
  *
  *   - [[AllMessage.AllEvent]] A regular [[Event]].
  *   - [[AllMessage.LastAllStreamPosition]] The last position in the global, [[sec.StreamId.All]], stream.
  */
sealed trait AllMessage
object AllMessage:

  final case class AllEvent(event: Event) extends AllMessage
  final case class LastAllStreamPosition(position: LogPosition) extends AllMessage

  //

  extension (am: AllMessage)

    def fold[A](
      aeFn: AllEvent => A,
      lpFn: LastAllStreamPosition => A
    ): A = am match
      case x: AllEvent              => aeFn(x)
      case x: LastAllStreamPosition => lpFn(x)

    def event: Option[AllEvent]             = fold(_.some, _ => none)
    def last: Option[LastAllStreamPosition] = fold(_ => none, _.some)

    def isEvent: Boolean = event.isDefined
    def isLast: Boolean  = last.isDefined
