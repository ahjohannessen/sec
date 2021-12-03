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

import cats.MonadThrow
import cats.data.OptionT
import cats.syntax.all._
import cats.effect.Temporal
import fs2.{Pipe, Stream}
import com.eventstore.dbclient.proto.streams._
import sec.api.mapping.streams.{incoming => mi}
import sec.api.mapping.streams.{outgoing => mo}

/** Low-level API for reading streams that exposes more detailed messages than the [[Streams]] API.
  *
  * @tparam F
  *   the effect type in which [[Reads]] operates.
  */
trait Reads[F[_]] {

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
    *   a [[Stream]] that emits [[AllMessage]] values.
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
    *   a [[Stream]] that emits [[StreamMessage]] values.
    */
  def readStreamMessages(
    streamId: StreamId,
    from: StreamPosition,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean
  ): Stream[F, StreamMessage]

}

object Reads {

  private[sec] def apply[F[_]: Temporal, C](
    client: StreamsFs2Grpc[F, C],
    mkCtx: Option[UserCredentials] => C
  ): Reads[F] = new Reads[F] {

    val readHandle: ReadReq => Stream[F, ReadResp] =
      client.read(_, mkCtx(None))

    def readAllMessages(
      from: LogPosition,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean
    ): Stream[F, AllMessage] =
      ReadAll(from, direction, maxCount, resolveLinkTos, readHandle).run
        .through(ReadAll.messagePipe[F])

    def readStreamMessages(
      streamId: StreamId,
      from: StreamPosition,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean
    ): Stream[F, StreamMessage] =
      ReadStream(streamId, from, direction, maxCount, resolveLinkTos, readHandle).run
        .through(ReadStream.messagePipe[F])

  }

}

// ====================================================================================================================

final private[api] case class ReadAll[F[_]](
  from: LogPosition,
  direction: Direction,
  maxCount: Long,
  resolveLinkTos: Boolean,
  handle: ReadReq => Stream[F, ReadResp]
)

private[api] object ReadAll {

  implicit final class ReadAllOps[F[_]](val ra: ReadAll[F]) extends AnyVal {

    def isValid: Boolean                      = ra.maxCount > 0L
    def withFrom(lp: LogPosition): ReadAll[F] = ra.copy(from = lp)

    def toReadReq: ReadReq =
      mo.mkReadAllReq(ra.from, ra.direction, ra.maxCount, ra.resolveLinkTos)

    def run: Stream[F, ReadResp] =
      if (isValid) ra.handle(ra.toReadReq) else Stream.empty

    def runFrom(lp: LogPosition): Stream[F, ReadResp] =
      ra.withFrom(lp).run

  }

  //

  def messagePipe[F[_]: MonadThrow]: Pipe[F, ReadResp, AllMessage] = _.evalMapFilter { r =>

    val e  = OptionT(r.content.event.flatTraverse(mi.mkAllMessageEvent[F](_))).widen[AllMessage]
    val lp = OptionT(r.content.lastAllStreamPosition.traverse(mi.mkAllMessageLast[F])).widen[AllMessage]

    (e <+> lp).value
  }

}

final private[api] case class ReadStream[F[_]](
  streamId: StreamId,
  from: StreamPosition,
  direction: Direction,
  maxCount: Long,
  resolveLinkTos: Boolean,
  handle: ReadReq => Stream[F, ReadResp]
)

private[api] object ReadStream {

  implicit final class ReadStreamOps[F[_]](val rs: ReadStream[F]) extends AnyVal {

    def isValid: Boolean =
      rs.direction.fold(rs.from =!= StreamPosition.End, true) && rs.maxCount > 0L

    def withFrom(sp: StreamPosition): ReadStream[F] =
      rs.copy(from = sp)

    def toReadReq: ReadReq =
      mo.mkReadStreamReq(rs.streamId, rs.from, rs.direction, rs.maxCount, rs.resolveLinkTos)

    def run: Stream[F, ReadResp] =
      if (isValid) rs.handle(rs.toReadReq) else Stream.empty

    def runFrom(sp: StreamPosition): Stream[F, ReadResp] =
      rs.withFrom(sp).run
  }

  //

  def messagePipe[F[_]: MonadThrow]: Pipe[F, ReadResp, StreamMessage] = _.evalMapFilter { r =>

    val e  = OptionT(r.content.event.flatTraverse(mi.mkStreamMessageEvent[F](_))).widen[StreamMessage]
    val nf = OptionT(r.content.streamNotFound.traverse(mi.mkStreamMessageNotFound[F])).widen[StreamMessage]
    val fp = OptionT(r.content.firstStreamPosition.traverse(mi.mkStreamMessageFirst[F])).widen[StreamMessage]
    val lp = OptionT(r.content.lastStreamPosition.traverse(mi.mkStreamMessageLast[F])).widen[StreamMessage]

    (e <+> fp <+> lp <+> nf).value
  }

}
