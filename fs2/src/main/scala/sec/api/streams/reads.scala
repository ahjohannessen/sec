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
package streams

import cats.syntax.all.*
import cats.effect.Temporal
import fs2.Stream
import com.eventstore.dbclient.proto.streams.*
import sec.api.mapping.streams.incoming as mi
import sec.api.mapping.streams.outgoing as mo

/** Low-level API for reading streams that exposes more detailed messages than the [[Streams]] API.
  *
  * @tparam F
  *   the effect type in which [[Reads]] operates.
  */
trait Reads[F[_]]:

  /** Read [[AllMessage]] messages from the global stream, [[sec.StreamId.All]].
    *
    * @param from
    *   log position to read from.
    * @param direction
    *   whether to read forwards or backwards.
    * @param maxCount
    *   limits maximum events returned.
    * @param resolveLinkTos
    *   whether to resolve [[EventType.LinkTo]] events automatically.
    * @return
    *   a [[fs2.Stream]] that emits [[sec.api.AllMessage]] values.
    */
  def readAllMessages(
    from: LogPosition,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean
  ): Stream[F, AllMessage]

  /** Read [[StreamMessage]] messages from an individual stream.
    *
    * @param streamId
    *   the id of the stream to subscribe to.
    * @param from
    *   stream position to read from.
    * @param direction
    *   whether to read forwards or backwards.
    * @param maxCount
    *   limits maximum events returned.
    * @param resolveLinkTos
    *   whether to resolve [[EventType.LinkTo]] events automatically.
    * @return
    *   a [[fs2.Stream]] that emits [[sec.api.StreamMessage]] values.
    */
  def readStreamMessages(
    streamId: StreamId,
    from: StreamPosition,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean
  ): Stream[F, StreamMessage]

object Reads:

  private[sec] def apply[F[_]: Temporal, C](
    client: StreamsFs2Grpc[F, C],
    mkCtx: Option[UserCredentials] => C
  ): Reads[F] = new Reads[F]:

    val readAll: ReadReq => Stream[F, mi.AllResult] =
      client.read(_, mkCtx(None)).evalMap(mi.AllResult.fromWire[F])

    val readStream: ReadReq => Stream[F, mi.StreamResult] =
      client.read(_, mkCtx(None)).evalMap(mi.StreamResult.fromWire[F])

    def readAllMessages(
      from: LogPosition,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean
    ): Stream[F, AllMessage] =
      ReadAll(from, direction, maxCount, resolveLinkTos, readAll).run
        .mapFilter(_.toAllMessage)

    def readStreamMessages(
      streamId: StreamId,
      from: StreamPosition,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean
    ): Stream[F, StreamMessage] =
      ReadStream(streamId, from, direction, maxCount, resolveLinkTos, readStream).run
        .mapFilter(_.toStreamMessage)

// ====================================================================================================================

final private[api] case class ReadAll[F[_]](
  from: LogPosition,
  direction: Direction,
  maxCount: Long,
  resolveLinkTos: Boolean,
  handle: ReadReq => Stream[F, mi.AllResult]
)

private[api] object ReadAll:

  extension [F[_]](ra: ReadAll[F])

    def withFrom(lp: LogPosition): ReadAll[F] =
      ra.copy(from = lp)

    def run: Stream[F, mi.AllResult] =
      val valid = ra.maxCount > 0L
      val req   = mo.mkReadAllReq(ra.from, ra.direction, ra.maxCount, ra.resolveLinkTos)
      if valid then ra.handle(req) else Stream.empty

final private[api] case class ReadStream[F[_]](
  streamId: StreamId,
  from: StreamPosition,
  direction: Direction,
  maxCount: Long,
  resolveLinkTos: Boolean,
  handle: ReadReq => Stream[F, mi.StreamResult]
)

private[api] object ReadStream:

  extension [F[_]](rs: ReadStream[F])

    def withFrom(sp: StreamPosition): ReadStream[F] =
      rs.copy(from = sp)

    def run: Stream[F, mi.StreamResult] =
      val valid = rs.direction.fold(rs.from =!= StreamPosition.End, true) && rs.maxCount > 0L
      val req   = mo.mkReadStreamReq(rs.streamId, rs.from, rs.direction, rs.maxCount, rs.resolveLinkTos)
      if valid then rs.handle(req) else Stream.empty
