/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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

import scala.concurrent.duration.*
import scala.util.control.NonFatal
import cats.*
import cats.data.*
import cats.effect.*
import cats.syntax.all.*
import io.kurrent.dbclient.proto.streams.*
import fs2.{Pipe, Pull, Stream}
import org.typelevel.log4cats.Logger
import sec.api.exceptions.{ResubscriptionRequired, WrongExpectedVersion}
import sec.api.mapping.streams.incoming.*
import sec.api.mapping.streams.outgoing.*
import sec.api.streams.*

/** API for interacting with streams in KurrentDB.
  *
  * ==Main operations==
  *
  *   - subscribing to the global stream or an individual stream.
  *   - reading from the global stream or an individual stream.
  *   - appending event data to an existing stream or creating a new stream.
  *   - deleting events from a stream.
  *
  * @tparam F
  *   the effect type in which [[Streams]] operates.
  */
trait Streams[F[_]]:

  /** Subscribes to the global stream, [[StreamId.All]].
    *
    * @param exclusiveFrom
    *   position to start from. Use [[scala.None]] to subscribe from the beginning.
    * @param resolveLinkTos
    *   whether to resolve [[EventType.LinkTo]] events automatically.
    * @return
    *   a [[fs2.Stream]] that emits [[Event]] values.
    */
  def subscribeToAll(
    exclusiveFrom: Option[LogPosition],
    resolveLinkTos: Boolean
  ): Stream[F, Event]

  /** Subscribes to the global stream, [[StreamId.All]] using a subscription filter.
    *
    * @param exclusiveFrom
    *   log position to start from. Use [[scala.None]] to subscribe from the beginning.
    * @param filterOptions
    *   to use when subscribing - See [[sec.api.SubscriptionFilterOptions]].
    * @param resolveLinkTos
    *   whether to resolve [[EventType.LinkTo]] events automatically.
    * @return
    *   a [[fs2.Stream]] that emits either [[sec.api.Checkpoint]] or [[Event]] values. How frequent
    *   [[sec.api.Checkpoint]] is emitted depends on `filterOptions`.
    */
  def subscribeToAll(
    exclusiveFrom: Option[LogPosition],
    filterOptions: SubscriptionFilterOptions,
    resolveLinkTos: Boolean
  ): Stream[F, Either[Checkpoint, Event]]

  /** Subscribes to an individual stream.
    *
    * @param streamId
    *   the id of the stream to subscribe to.
    * @param exclusiveFrom
    *   stream position to start from. Use [[scala.None]] to subscribe from the beginning.
    * @param resolveLinkTos
    *   whether to resolve [[EventType.LinkTo]] events automatically.
    * @return
    *   a [[fs2.Stream]] that emits [[Event]] values.
    */
  def subscribeToStream(
    streamId: StreamId,
    exclusiveFrom: Option[StreamPosition],
    resolveLinkTos: Boolean
  ): Stream[F, Event]

  /** Read events from the global stream, [[sec.StreamId.All]].
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
    *   a [[fs2.Stream]] that emits [[Event]] values.
    */
  def readAll(
    from: LogPosition,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean
  ): Stream[F, Event]

  /** Read events from an individual stream. A [[sec.api.exceptions.StreamNotFound]] is raised when the stream does not
    * exist.
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
    *   a [[fs2.Stream]] that emits [[Event]] values.
    */
  def readStream(
    streamId: StreamId,
    from: StreamPosition,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean
  ): Stream[F, Event]

  /** Appends [[EventData]] to a stream and returns [[WriteResult]] with current positions of the stream after a
    * successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @see
    *   [[https://ahjohannessen.github.io/sec/docs/writing]] for details about appending to a stream.
    *
    * @param streamId
    *   the id of the stream to append to.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    * @param data
    *   event data to be appended to the stream. See [[EventData]].
    */
  def appendToStream(
    streamId: StreamId,
    expectedState: StreamState,
    data: NonEmptyList[EventData]
  ): F[WriteResult]

  /** Deletes a stream and returns [[DeleteResult]] with current log position after a successful operation. Failure to
    * fulfill the expected state is manifested by raising [[sec.api.exceptions.WrongExpectedState]].
    *
    * @note
    *   Deleted streams can be recreated.
    * @see
    *   [[https://ahjohannessen.github.io/sec/docs/deleting]] for details about what it means to delete a stream.
    *
    * @param streamId
    *   the id of the stream to delete.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    */
  def delete(
    streamId: StreamId,
    expectedState: StreamState
  ): F[DeleteResult]

  /** Tombstones a stream and returns [[TombstoneResult]] with current log position after a successful operation.
    * Failure to fulfill the expected state is manifested by raising [[sec.api.exceptions.WrongExpectedState]].
    *
    * @note
    *   Tombstoned streams can *never* be recreated.
    * @see
    *   [[https://ahjohannessen.github.io/sec/docs/deleting]] for details about what it means to tombstone a stream.
    *
    * @param streamId
    *   the id of the stream to delete.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    */
  def tombstone(
    streamId: StreamId,
    expectedState: StreamState
  ): F[TombstoneResult]

  /** Returns an instance that uses provided [[UserCredentials]]. This is useful when an operation requires different
    * credentials from what is provided through configuration.
    *
    * @param creds
    *   Custom user credentials to use.
    */
  def withCredentials(
    creds: UserCredentials
  ): Streams[F]

  /** Returns an [[sec.api.streams.Reads]] instance. This is useful when you need more granularity from `readAll` or
    * `readStream` operations.
    */
  def messageReads: Reads[F]

object Streams:

//======================================================================================================================

  private[sec] def apply[F[_]: Temporal, C](
    client: StreamsFs2Grpc[F, C],
    mkCtx: Option[UserCredentials] => C,
    opts: Opts[F]
  ): Streams[F] = new Streams[F]:

    private given C = mkCtx(None)
    private val read   = client.read(_)
    private val append = client.append(_)
    private val delete = client.delete(_)
    private val tomb   = client.tombstone(_)

    private val subscriptionOpts: Opts[F] =
      opts.copy(retryOn = th => opts.retryOn(th) || th.isInstanceOf[ResubscriptionRequired])

    def subscribeToAll(
      exclusiveFrom: Option[LogPosition],
      resolveLinkTos: Boolean
    ): Stream[F, Event] =
      subscribeToAll0[F](exclusiveFrom, resolveLinkTos, subscriptionOpts)(read)

    def subscribeToAll(
      exclusiveFrom: Option[LogPosition],
      filterOptions: SubscriptionFilterOptions,
      resolveLinkTos: Boolean
    ): Stream[F, Either[Checkpoint, Event]] =
      subscribeToAll0[F](exclusiveFrom, filterOptions, resolveLinkTos, subscriptionOpts)(read)

    def subscribeToStream(
      streamId: StreamId,
      exclusiveFrom: Option[StreamPosition],
      resolveLinkTos: Boolean
    ): Stream[F, Event] =
      subscribeToStream0[F](streamId, exclusiveFrom, resolveLinkTos, subscriptionOpts)(read)

    def readAll(
      from: LogPosition,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean
    ): Stream[F, Event] =

      val read: LogPosition => Stream[F, Event] = messageReads
        .readAllMessages(_, direction, maxCount, resolveLinkTos)
        .mapFilter(_.event.map(_.event))

      withRetry[F, LogPosition, Event](from, read, _.record.logPosition, opts, "readAll", direction)

    def readStream(
      streamId: StreamId,
      from: StreamPosition,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean
    ): Stream[F, Event] =

      val read: StreamPosition => Stream[F, Event] = messageReads
        .readStreamMessages(streamId, _, direction, maxCount, resolveLinkTos)
        .evalTap(sm => sm.notFound.fold(sm.pure[F])(_.toException.raiseError))
        .mapFilter(_.event.map(_.event))

      withRetry[F, StreamPosition, Event](from, read, _.record.streamPosition, opts, "readStream", direction)

    def appendToStream(
      streamId: StreamId,
      expectedState: StreamState,
      data: NonEmptyList[EventData]
    ): F[WriteResult] =
      appendToStream0[F](streamId, expectedState, data, opts)(append)

    def delete(
      streamId: StreamId,
      expectedState: StreamState
    ): F[DeleteResult] =
      delete0[F](streamId, expectedState, opts)(delete)

    def tombstone(
      streamId: StreamId,
      expectedState: StreamState
    ): F[TombstoneResult] =
      tombstone0[F](streamId, expectedState, opts)(tomb)

    def withCredentials(
      creds: UserCredentials
    ): Streams[F] =
      Streams[F, C](client, _ => mkCtx(creds.some), opts)

    val messageReads: Reads[F] =
      Reads[F, C](client, mkCtx)

//======================================================================================================================

  private[sec] def subscribeToAll0[F[_]: Temporal](
    exclusiveFrom: Option[LogPosition],
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, Event] =

    val opName: String                               = "subscribeToAll"
    val pipeLog: Logger[F]                           = opts.log.withModifiedString(m => s"$opName: $m")
    val mkReq: Option[LogPosition] => ReadReq        = mkSubscribeToAllReq(_, resolveLinkTos, None)
    val sub: Option[LogPosition] => Stream[F, Event] = ef => f(mkReq(ef)).through(subscriptionAllPipe(pipeLog))
    val fn: Event => Option[LogPosition]             = _.record.logPosition.some

    withRetry(exclusiveFrom, sub, fn, opts, opName, Direction.Forwards)

  private[sec] def subscribeToAll0[F[_]: Temporal](
    exclusiveFrom: Option[LogPosition],
    filterOptions: SubscriptionFilterOptions,
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, Either[Checkpoint, Event]] =

    type O = Either[Checkpoint, Event]

    val opName: String                           = "subscribeToAllWithFilter"
    val pipeLog: Logger[F]                       = opts.log.withModifiedString(m => s"$opName: $m")
    val mkReq: Option[LogPosition] => ReadReq    = mkSubscribeToAllReq(_, resolveLinkTos, filterOptions.some)
    val sub: Option[LogPosition] => Stream[F, O] = ef => f(mkReq(ef)).through(subAllFilteredPipe(pipeLog))
    val fn: O => Option[LogPosition]             = _.fold(_.logPosition, _.record.logPosition).some

    withRetry(exclusiveFrom, sub, fn, opts, opName, Direction.Forwards)

  private[sec] def subscribeToStream0[F[_]: Temporal](
    streamId: StreamId,
    exclusiveFrom: Option[StreamPosition],
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, Event] =

    val opName: String                           = "subscribeToStream"
    val pipeLog: Logger[F]                       = opts.log.withModifiedString(m => s"$opName[${streamId.render}]: $m")
    val mkReq: Option[StreamPosition] => ReadReq = mkSubscribeToStreamReq(streamId, _, resolveLinkTos)

    val sub: Option[StreamPosition] => Stream[F, Event] = ef => f(mkReq(ef)).through(subscriptionStreamPipe(pipeLog))

    val fn: Event => Option[StreamPosition] = _.record.streamPosition.some

    withRetry(exclusiveFrom, sub, fn, opts, opName, Direction.Forwards)

  private[sec] def appendToStream0[F[_]: Temporal](
    streamId: StreamId,
    expectedState: StreamState,
    eventsNel: NonEmptyList[EventData],
    opts: Opts[F]
  )(f: Stream[F, AppendReq] => F[AppendResp]): F[WriteResult] =

    val header: AppendReq        = mkAppendHeaderReq(streamId, expectedState)
    val events: List[AppendReq]  = mkAppendProposalsReq(eventsNel).toList
    val operation: F[AppendResp] = f(Stream.emit(header) ++ Stream.emits(events))

    opts.run(operation, "appendToStream") >>= { ar => mkWriteResult[F](streamId, ar) }

  private[sec] def delete0[F[_]: Temporal](
    streamId: StreamId,
    expectedState: StreamState,
    opts: Opts[F]
  )(f: DeleteReq => F[DeleteResp]): F[DeleteResult] =
    (opts.run(f(mkDeleteReq(streamId, expectedState)), "delete") >>= mkDeleteResult[F]).adaptError {
      case e: WrongExpectedVersion => e.adaptOrFallback(streamId, expectedState)
    }

  private[sec] def tombstone0[F[_]: Temporal](
    streamId: StreamId,
    expectedState: StreamState,
    opts: Opts[F]
  )(f: TombstoneReq => F[TombstoneResp]): F[TombstoneResult] =
    (opts.run(f(mkTombstoneReq(streamId, expectedState)), "tombstone") >>= mkTombstoneResult[F]).adaptError {
      case e: WrongExpectedVersion => e.adaptOrFallback(streamId, expectedState)
    }

//======================================================================================================================

  private[sec] def subConfirmationPipe[F[_]: MonadThrow](logger: Logger[F]): Pipe[F, ReadResp, ReadResp] = in =>

    val log: SubscriptionConfirmation => F[Unit] =
      sc => logger.debug(s"$sc received")

    val initialPull = in.pull.uncons1.flatMap {
      case Some((h, t)) => Pull.eval(reqConfirmation[F](h) >>= log) >> t.pull.echo
      case None         => Pull.done
    }

    initialPull.stream

  private[sec] def subscriptionAllPipe[F[_]: MonadThrow](
    log: Logger[F]
  ): Pipe[F, ReadResp, Event] =
    _.filterNot(_.isCheckpoint)
      .through(subConfirmationPipe(log))
      .through(_.filter(_.isEvent).evalMap(reqReadEvent[F]).unNone)

  private[sec] def subscriptionStreamPipe[F[_]: MonadThrow](
    log: Logger[F]
  ): Pipe[F, ReadResp, Event] =
    _.through(subConfirmationPipe(log))
      .through(_.filter(_.isEvent).evalMap(reqReadEvent[F]).unNone)

  private[sec] def subAllFilteredPipe[F[_]: MonadThrow](
    log: Logger[F]
  ): Pipe[F, ReadResp, Either[Checkpoint, Event]] =
    // Defensive filtering to guard against ESDB emitting checkpoints
    // with `LogPosition.Exact.MaxValue`, i.e. end of stream.
    _.through(subConfirmationPipe(log))
      .through(_.filter(_.isCheckpointOrEvent).evalMap(mkCheckpointOrEvent[F]).unNone)
      .collect {
        case r @ Right(_)                               => r
        case l @ Left(v) if v != Checkpoint.endOfStream => l
      }

  private[sec] def withRetry[F[_]: Temporal, T: Order, O](
    from: T,
    streamFn: T => Stream[F, O],
    extractFn: O => T,
    o: Opts[F],
    opName: String,
    direction: Direction
  ): Stream[F, O] =
    if o.retryEnabled then

      val logWarn     = o.logWarn(opName)
      val logError    = o.logError(opName)
      val nextDelay   = o.retryConfig.nextDelay
      val maxAttempts = o.retryConfig.maxAttempts
      val order       = Order[T]

      Stream.eval(Ref.of[F, Option[T]](None)) >>= { state =>

        val readFilter: O => F[Boolean] = o => {

          val next: T              = extractFn(o)
          val filter: T => Boolean = direction.fold(order.gt, order.lt)(next, _)

          state.get.map(_.fold(true)(filter))
        }

        def run(f: T, attempts: Int, d: FiniteDuration): Stream[F, O] =

          val readAndFilter: Stream[F, O] = streamFn(f).evalFilter(readFilter)
          val readAndUpdate: Stream[F, O] = readAndFilter.evalTap(o => state.set(extractFn(o).some))

          readAndUpdate.recoverWith {

            case NonFatal(t) if o.retryOn(t) =>
              if attempts <= maxAttempts then

                val logWarning = logWarn(attempts, d, t).whenA(attempts < maxAttempts)
                val getCurrent = state.get.map(_.getOrElse(f))

                Stream.eval(logWarning *> getCurrent) >>= { c =>
                  run(c, attempts + 1, nextDelay(d)).delayBy(d)
                }
              else Stream.eval(logError(t)) *> Stream.raiseError[F](t)
          }

        run(from, 1, o.retryConfig.delay)
      }
    else streamFn(from)

//======================================================================================================================

  extension (rr: ReadResp)
    private[sec] def isEvent: Boolean             = rr.content.isEvent
    private[sec] def isCheckpoint: Boolean        = rr.content.isCheckpoint
    private[sec] def isCheckpointOrEvent: Boolean = rr.isEvent || rr.isCheckpoint
