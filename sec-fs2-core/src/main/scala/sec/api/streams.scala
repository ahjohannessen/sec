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
import cats._
import cats.data._
import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent.Ref
import fs2.{Pipe, Pull, Stream}
import com.eventstore.dbclient.proto.streams._
import io.chrisdavenport.log4cats.Logger
import sec.api.exceptions.WrongExpectedVersion
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
  ): Stream[F, Either[Checkpoint, Event]]

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
  ): F[WriteResult]

  def delete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[DeleteResult]

  def tombstone(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[DeleteResult]

  private[sec] def metadata: MetaStreams[F]

}

object Streams {

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

    val opName: String                            = "subscribeToAll"
    val pipeLog: Logger[F]                        = opts.log.withModifiedString(m => s"$opName: $m")
    val mkReq: Option[Position] => ReadReq        = mkSubscribeToAllReq(_, resolveLinkTos, None)
    val sub: Option[Position] => Stream[F, Event] = ef => f(mkReq(ef)).through(subscriptionPipe(pipeLog))
    val fn: Event => Option[Position]             = _.record.position.some

    withRetry(exclusiveFrom, sub, fn, opts, opName, Direction.Forwards)

  }

  private[sec] def subscribeToAll0[F[_]: Sync: Timer](
    exclusiveFrom: Option[Position],
    filterOptions: SubscriptionFilterOptions,
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, Either[Checkpoint, Event]] = {

    type O = Either[Checkpoint, Event]

    val opName: String                        = "subscribeToAllWithFilter"
    val pipeLog: Logger[F]                    = opts.log.withModifiedString(m => s"$opName: $m")
    val mkReq: Option[Position] => ReadReq    = mkSubscribeToAllReq(_, resolveLinkTos, filterOptions.some)
    val sub: Option[Position] => Stream[F, O] = ef => f(mkReq(ef)).through(subAllFilteredPipe(pipeLog))
    val fn: O => Option[Position]             = _.fold(_.position, _.record.position).some

    withRetry(exclusiveFrom, sub, fn, opts, opName, Direction.Forwards)
  }

  private[sec] def subscribeToStream0[F[_]: Sync: Timer](
    streamId: StreamId,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, Event] = {

    val opName: String                               = "subscribeToStream"
    val pipeLog: Logger[F]                           = opts.log.withModifiedString(m => s"$opName[${streamId.show}]: $m")
    val mkReq: Option[EventNumber] => ReadReq        = mkSubscribeToStreamReq(streamId, _, resolveLinkTos)
    val sub: Option[EventNumber] => Stream[F, Event] = ef => f(mkReq(ef)).through(subscriptionPipe(pipeLog))
    val fn: Event => Option[EventNumber]             = _.record.number.some

    withRetry(exclusiveFrom, sub, fn, opts, opName, Direction.Forwards)
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

    if (maxCount > 0) withRetry(from, read, fn, opts, "readAll", direction) else Stream.empty
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

    if (valid && maxCount > 0) withRetry(from, read, fn, opts, "readStream", direction) else Stream.empty
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
    (opts.run(f(mkDeleteReq(streamId, expectedRevision)), "delete") >>= mkDeleteResult[F]).adaptError {
      case e: WrongExpectedVersion => e.adaptOrFallback(streamId, expectedRevision)
    }

  private[sec] def tombstone0[F[_]: Concurrent: Timer](
    streamId: StreamId,
    expectedRevision: StreamRevision,
    opts: Opts[F]
  )(f: TombstoneReq => F[TombstoneResp]): F[DeleteResult] =
    (opts.run(f(mkTombstoneReq(streamId, expectedRevision)), "tombstone") >>= mkDeleteResult[F]).adaptError {
      case e: WrongExpectedVersion => e.adaptOrFallback(streamId, expectedRevision)
    }

//======================================================================================================================

  private[sec] def readEventPipe[F[_]: ErrorM]: Pipe[F, ReadResp, Event] =
    _.evalMap(_.content.event.require[F]("ReadEvent expected!").flatMap(mkEvent[F])).unNone

  private[sec] def failStreamNotFound[F[_]: ErrorM]: Pipe[F, ReadResp, ReadResp] = _.evalMap { r =>
    r.content.streamNotFound.fold(r.pure[F])(mkStreamNotFound[F](_) >>= (_.raiseError[F, ReadResp]))
  }

  private[sec] def subConfirmationPipe[F[_]: ErrorM](log: Logger[F]): Pipe[F, ReadResp, ReadResp] = in => {

    val extractConfirmation: ReadResp => F[String] =
      _.content.confirmation.map(_.subscriptionId).require[F]("SubscriptionConfirmation expected!")

    val logConfirmation: String => F[Unit] =
      id => log.debug(s"SubscriptionConfirmation received, id: $id")

    val initialPull = in.pull.uncons1.flatMap {
      case Some((head, tail)) => Pull.eval(extractConfirmation(head) >>= logConfirmation) >> tail.pull.echo
      case None               => Pull.done
    }

    initialPull.stream
  }

  private[sec] def subscriptionPipe[F[_]: ErrorM](log: Logger[F]): Pipe[F, ReadResp, Event] =
    _.through(subConfirmationPipe(log)).through(readEventPipe)

  private[sec] def subAllFilteredPipe[F[_]: ErrorM](log: Logger[F]): Pipe[F, ReadResp, Either[Checkpoint, Event]] =
    _.through(subConfirmationPipe(log)).through(_.evalMap(mkCheckpointOrEvent[F]).unNone)

  private[sec] def withRetry[F[_]: Sync: Timer, T: Order, O](
    from: T,
    streamFn: T => Stream[F, O],
    extractFn: O => T,
    o: Opts[F],
    opName: String,
    direction: Direction
  ): Stream[F, O] = {

    if (o.retryEnabled) {

      val logWarn     = o.logWarn(opName) _
      val logError    = o.logError(opName) _
      val nextDelay   = o.retryConfig.nextDelay _
      val maxAttempts = o.retryConfig.maxAttempts
      val order       = Order[T]

      Stream.eval(Ref.of[F, Option[T]](None)) >>= { state =>

        val readFilter: O => F[Boolean] = o => {

          val next: T              = extractFn(o)
          val filter: T => Boolean = direction.fold(order.gt _, order.lt _)(next, _)

          state.get.map(_.fold(true)(filter))
        }

        def run(f: T, attempts: Int, d: FiniteDuration): Stream[F, O] = {

          val readAndFilter: Stream[F, O] = streamFn(f).evalFilter(readFilter)
          val readAndUpdate: Stream[F, O] = readAndFilter.evalTap(o => state.set(extractFn(o).some))

          readAndUpdate.recoverWith {

            case NonFatal(t) if o.retryOn(t) =>
              if (attempts <= maxAttempts) {

                val logWarning = logWarn(attempts, d, t).whenA(attempts < maxAttempts)
                val getCurrent = state.get.map(_.getOrElse(f))

                Stream.eval(logWarning *> getCurrent) >>= { c =>
                  run(c, attempts + 1, nextDelay(d)).delayBy(d)
                }

              } else
                Stream.eval(logError(t)) *> Stream.raiseError[F](t)
          }
        }

        run(from, 1, o.retryConfig.delay)
      }

    } else streamFn(from)

  }

}
