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
import _root_.sec.api._
import _root_.sec.api.streams.Reads

trait ReadsSyntax {
  implicit final def syntaxForReads[F[_]](r: Reads[F]): ReadsOps[F] = new ReadsOps[F](r)
}

final class ReadsOps[F[_]](val r: Reads[F]) extends AnyVal {

  /** Read [[AllMessage]] messages forwards from the global stream, [[sec.StreamId.All]].
    *
    * @param from
    *   log position to read from.
    * @param maxCount
    *   limits maximum events returned.
    * @param resolveLinkTos
    *   whether to resolve [[EventType.LinkTo]] events automatically.
    * @return
    *   a [[Stream]] that emits [[AllMessage]] values.
    */
  def readAllMessagesForwards(
    from: LogPosition = LogPosition.Start,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, AllMessage] =
    r.readAllMessages(from, Direction.Forwards, maxCount, resolveLinkTos)

  /** Read [[AllMessage]] messages backwards from the global stream, [[sec.StreamId.All]].
    *
    * @param from
    *   log position to read from.
    * @param maxCount
    *   limits maximum events returned.
    * @param resolveLinkTos
    *   whether to resolve [[EventType.LinkTo]] events automatically.
    * @return
    *   a [[Stream]] that emits [[AllMessage]] values.
    */
  def readAllMessagesBackwards(
    from: LogPosition = LogPosition.End,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, AllMessage] =
    r.readAllMessages(from, Direction.Backwards, maxCount, resolveLinkTos)

  /** Read [[StreamMessage]] messages forwards from an individual stream.
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
    *   a [[Stream]] that emits [[StreamMessage]] values.
    */
  def readStreamMessagesForwards(
    streamId: StreamId,
    from: StreamPosition = StreamPosition.Start,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, StreamMessage] =
    r.readStreamMessages(streamId, from, Direction.Forwards, maxCount, resolveLinkTos)

  /** Read [[StreamMessage]] messages backwards from an individual stream.
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
    *   a [[Stream]] that emits [[StreamMessage]] values.
    */
  def readStreamMessagesBackwards(
    streamId: StreamId,
    from: StreamPosition = StreamPosition.End,
    maxCount: Long = Long.MaxValue,
    resolveLinkTos: Boolean = false
  ): Stream[F, StreamMessage] =
    r.readStreamMessages(streamId, from, Direction.Backwards, maxCount, resolveLinkTos)

}
