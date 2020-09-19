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

import scala.concurrent.duration._
import scala.util.control.NonFatal
import cats.Eq
import cats.data._
import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent.Ref
import fs2.{Pipe, Pull, Stream}
import com.eventstore.client.streams._
import sec.core._
import sec.api.exceptions.StreamNotFound
import sec.api.mapping.shared._
import sec.api.mapping.streams.outgoing._
import sec.api.mapping.streams.incoming._
import sec.api.mapping.implicits._

trait Streams[F[_]] {

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, Event]

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    filterOptions: SubscriptionFilterOptions,
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, Either[Streams.Checkpoint, Event]]

  def subscribeToStream(
    streamId: StreamId,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, Event]

  def readAll(
    from: Position,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, Event]

  def readStream(
    streamId: StreamId,
    from: EventNumber,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, Event]

  def appendToStream(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    events: NonEmptyList[EventData],
    creds: Option[UserCredentials]
  ): F[Streams.WriteResult]

  def delete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[Streams.DeleteResult]

  def tombstone(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[Streams.DeleteResult]

  private[sec] def metadata: MetaStreams[F]

}

object Streams {

  type Checkpoint = Position
  final case class WriteResult(currentRevision: EventNumber.Exact, position: Position.Exact)
  final case class DeleteResult(position: Position.Exact)

//======================================================================================================================

  private[sec] def apply[F[_]: Concurrent: Timer, C](
    client: StreamsFs2Grpc[F, C],
    mkCtx: Option[UserCredentials] => C,
    opts: Opts[F]
  ): Streams[F] = new Streams[F] {

    def subscribeToAll(
      exclusiveFrom: Option[Position],
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] =
      subscribeToAll0[F](exclusiveFrom, resolveLinkTos, opts)(client.read(_, mkCtx(creds)))

    def subscribeToAll(
      exclusiveFrom: Option[Position],
      filterOptions: SubscriptionFilterOptions,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Either[Checkpoint, Event]] =
      subscribeToAll0[F](exclusiveFrom, filterOptions, resolveLinkTos, opts)(client.read(_, mkCtx(creds)))

    def subscribeToStream(
      streamId: StreamId,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] =
      subscribeToStream0[F](streamId, exclusiveFrom, resolveLinkTos, opts)(client.read(_, mkCtx(creds)))

    def readAll(
      from: Position,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] =
      readAll0[F](from, direction, maxCount, resolveLinkTos, opts)(client.read(_, mkCtx(creds)))

    def readStream(
      streamId: StreamId,
      from: EventNumber,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] =
      readStream0[F](streamId, from, direction, maxCount, resolveLinkTos, opts)(client.read(_, mkCtx(creds)))

    def appendToStream(
      streamId: StreamId,
      expectedRevision: StreamRevision,
      events: NonEmptyList[EventData],
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      appendToStream0[F](streamId, expectedRevision, events, opts)(client.append(_, mkCtx(creds)))

    def delete(
      streamId: StreamId,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[DeleteResult] =
      delete0[F](streamId, expectedRevision, opts)(client.delete(_, mkCtx(creds)))

    def tombstone(
      streamId: StreamId,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[DeleteResult] =
      tombstone0[F](streamId, expectedRevision, opts)(client.tombstone(_, mkCtx(creds)))

    private[sec] val metadata: MetaStreams[F] = MetaStreams[F](this)
  }

//======================================================================================================================

  private[sec] def subscribeToAll0[F[_]: Sync: Timer](
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, Event] = {

    val mkReq: Option[Position] => ReadReq        = mkSubscribeToAllReq(_, resolveLinkTos, None)
    val sub: Option[Position] => Stream[F, Event] = ef => f(mkReq(ef)).through(subscriptionPipe)
    val fn: Event => Option[Position]             = _.record.position.some

    withRetryS(exclusiveFrom, sub, fn, opts, "subscribeToAll")

  }

  private[sec] def subscribeToAll0[F[_]: Sync: Timer](
    exclusiveFrom: Option[Position],
    filterOptions: SubscriptionFilterOptions,
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, Either[Checkpoint, Event]] = {

    type O = Either[Checkpoint, Event]

    val mkReq: Option[Position] => ReadReq    = mkSubscribeToAllReq(_, resolveLinkTos, filterOptions.some)
    val sub: Option[Position] => Stream[F, O] = ef => f(mkReq(ef)).through(subAllFilteredPipe)
    val fn: O => Option[Position]             = _.fold(identity, _.record.position).some

    withRetryS(exclusiveFrom, sub, fn, opts, "subscribeToAll")
  }

  private[sec] def subscribeToStream0[F[_]: Sync: Timer](
    streamId: StreamId,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, Event] = {

    val mkReq: Option[EventNumber] => ReadReq        = mkSubscribeToStreamReq(streamId, _, resolveLinkTos)
    val sub: Option[EventNumber] => Stream[F, Event] = ef => f(mkReq(ef)).through(subscriptionPipe)
    val fn: Event => Option[EventNumber]             = _.record.number.some

    withRetryS(exclusiveFrom, sub, fn, opts, "subscribeToStream")
  }

  private[sec] def readAll0[F[_]: Sync: Timer](
    from: Position,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, Event] = {

    val mkReq: Position => ReadReq         = mkReadAllReq(_, direction, maxCount, resolveLinkTos)
    val read: Position => Stream[F, Event] = p => f(mkReq(p)).through(readEventPipe).take(maxCount)
    val fn: Event => Position              = _.record.position

    if (maxCount > 0) withRetryS(from, read, fn, opts, "readAll") else Stream.empty
  }

  private[sec] def readStream0[F[_]: Sync: Timer](
    streamId: StreamId,
    from: EventNumber,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, Event] = {

    val valid: Boolean                        = direction.fold(from =!= EventNumber.End, true)
    val mkReq: EventNumber => ReadReq         = mkReadStreamReq(streamId, _, direction, maxCount, resolveLinkTos)
    val read: EventNumber => Stream[F, Event] = e => f(mkReq(e)).through(failStreamNotFound).through(readEventPipe)
    val fn: Event => EventNumber              = _.record.number

    if (valid && maxCount > 0) withRetryS(from, read, fn, opts, "readStream") else Stream.empty
  }

  private[sec] def appendToStream0[F[_]: Concurrent: Timer](
    streamId: StreamId,
    expectedRevision: StreamRevision,
    eventsNel: NonEmptyList[EventData],
    opts: Opts[F]
  )(f: Stream[F, AppendReq] => F[AppendResp]): F[WriteResult] = {

    val header: AppendReq        = mkAppendHeaderReq(streamId, expectedRevision)
    val events: List[AppendReq]  = mkAppendProposalsReq(eventsNel).toList
    val operation: F[AppendResp] = f(Stream.emit(header) ++ Stream.emits(events))

    opts.run(operation, "appendToStream") >>= { ar => mkWriteResult[F](streamId, ar) }
  }

  private[sec] def delete0[F[_]: Concurrent: Timer](
    streamId: StreamId,
    expectedRevision: StreamRevision,
    opts: Opts[F]
  )(f: DeleteReq => F[DeleteResp]): F[DeleteResult] =
    opts.run(f(mkDeleteReq(streamId, expectedRevision)), "delete") >>= mkDeleteResult[F]

  private[sec] def tombstone0[F[_]: Concurrent: Timer](
    streamId: StreamId,
    expectedRevision: StreamRevision,
    opts: Opts[F]
  )(f: TombstoneReq => F[TombstoneResp]): F[DeleteResult] =
    opts.run(f(mkTombstoneReq(streamId, expectedRevision)), "tombstone") >>= mkDeleteResult[F]

//======================================================================================================================

  private[sec] def readEventPipe[F[_]: ErrorM]: Pipe[F, ReadResp, Event] =
    _.evalMap(_.content.event.require[F]("ReadEvent expected!").flatMap(mkEvent[F])).unNone

  private[sec] def failStreamNotFound[F[_]: ErrorM]: Pipe[F, ReadResp, ReadResp] = _.evalMap { r =>
    r.content.streamNotFound.fold(r.pure[F])(
      _.streamIdentifier
        .require[F]("StreamIdentifer expected!")
        .flatMap(_.utf8[F].map(StreamNotFound))
        .flatMap(_.raiseError[F, ReadResp])
    )
  }

  private[sec] def subConfirmationPipe[F[_]: ErrorM]: Pipe[F, ReadResp, ReadResp] = in => {

    val extractConfirmation: ReadResp => F[String] =
      _.content.confirmation.map(_.subscriptionId).require[F]("SubscriptionConfirmation expected!")

    val initialPull = in.pull.uncons1.flatMap {
      case Some((hd, tail)) => Pull.eval(extractConfirmation(hd)) >> tail.pull.echo
      case None             => Pull.done
    }

    initialPull.stream
  }

  private[sec] def subscriptionPipe[F[_]: ErrorM]: Pipe[F, ReadResp, Event] =
    _.through(subConfirmationPipe).through(readEventPipe)

  private[sec] def subAllFilteredPipe[F[_]](implicit F: ErrorM[F]): Pipe[F, ReadResp, Either[Position, Event]] = {

    import ReadResp._

    type R = Option[Either[Position, Event]]

    val checkpointOrEvent: Stream[F, ReadResp] => Stream[F, Either[Position, Event]] =
      _.evalMap(_.content match {
        case Content.Event(e)      => mkEvent[F](e).map[R](_.map(_.asRight))
        case Content.Checkpoint(c) => F.pure[R](Position.exact(c.commitPosition, c.preparePosition).asLeft.some)
        case _                     => F.pure[R](None)
      }).unNone

    _.through(subConfirmationPipe).through(checkpointOrEvent)
  }

  // TODO: Tests
  private[sec] def withRetryS[F[_]: Sync: Timer, T: Eq, O](
    from: T,
    streamFn: T => Stream[F, O],
    extractFn: O => T,
    o: Opts[F],
    opName: String
  ): Stream[F, O] = {

    val logWarn   = o.logWarn(opName) _
    val logError  = o.logError(opName) _
    val nextDelay = o.retryConfig.nextDelay _

    def run(f: T, attempts: Int, d: FiniteDuration): Stream[F, O] =
      Stream.eval(Ref.of[F, T](f)).flatMap { r =>
        streamFn(f).changesBy(extractFn).evalTap(o => r.set(extractFn(o))).recoverWith {
          case NonFatal(t) if o.retryOn(t) =>
            val attempt = attempts + 1
            if (attempt < o.retryConfig.maxAttempts)
              Stream.eval(logWarn(attempt, d, t) *> r.get).flatMap(run(_, attempt, nextDelay(d)).delayBy(d))
            else
              Stream.eval(logError(t)) *> Stream.raiseError[F](t)
        }
      }

    run(from, 0, o.retryConfig.delay)
  }

//======================================================================================================================

  /**
   * Convenience methods for [[sec.api.Streams]].
   */
  implicit final class StreamsSyntax[F[_]](s: Streams[F]) {

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
    ): Stream[F, Either[Position, Event]] =
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

}
