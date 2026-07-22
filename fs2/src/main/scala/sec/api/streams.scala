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
import io.kurrentdb.protocol.v2.streams.StreamsServiceFs2Grpc
import io.kurrentdb.protocol.v2.streams.{AppendRecordsRequest as V2AppendReq, AppendRecordsResponse as V2AppendResp}
import fs2.{Pipe, Pull, RaiseThrowable, Stream}
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

  /** Experimental - the underlying v2 protocol is marked unstable by KurrentDB and may change;
    * this API may change with it.
    *
    * Appends records to one or more streams atomically; the server must support and have enabled
    * the v2 protocol. Every written stream carries a mandatory [[StreamState]] expectation, and
    * [[sec.api.StreamGuard]] expresses conditions on streams that are read from but not written
    * to: below, the reservation is appended only if the flight stream is still at stream position 42,
    * although nothing is written to it (dynamic consistency boundary).
    *
    * {{{
    * client.streams.multiStreamAppend(
    *   appends = NonEmptyList.one(StreamAppend(reservationId, StreamState.NoStream, records)),
    *   guards  = List(StreamGuard(flightId, StreamPosition(42L)))
    * )
    * }}}
    *
    * Failure to fulfill a check is manifested by raising
    * [[sec.api.exceptions.AppendConsistencyViolation]]. A stream may appear only once across
    * appends and guards; a duplicate is rejected by the server. A
    * [[StreamState.Any]] expectation produces no check on the wire - the v2 protocol expresses
    * Any as absence - which also renders an Any guard vacuous.
    *
    * @param appends
    *   the streams to append to, each with an expectation and its records. See [[sec.api.StreamAppend]].
    * @param guards
    *   consistency conditions on unwritten streams. See [[sec.api.StreamGuard]].
    */
  def multiStreamAppend(
    appends: NonEmptyList[StreamAppend],
    guards: List[StreamGuard] = Nil
  ): F[MultiAppendResult]

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
    *
    * @note
    *   [[sec.api.streams.Reads]] always uses the regular channel. When a subscription pool is configured, a hand-rolled
    *   infinite read built on top of [[sec.api.streams.Reads]] bypasses the pool's concurrent-stream protection - use
    *   the `subscribeTo*` operations for long-lived streams.
    */
  def messageReads: Reads[F]

object Streams:

//======================================================================================================================

  private[sec] def apply[F[_]: Temporal, C](
    client: StreamsFs2Grpc[F, C],
    clientV2: StreamsServiceFs2Grpc[F, C],
    mkCtx: Option[UserCredentials] => C,
    opts: Opts[F],
    subscriptionTransport: Option[(ReadReq, C) => Stream[F, ReadResp]] = None
  ): Streams[F] = new Streams[F]:

    private given C = mkCtx(None)
    private val read     = client.read(_)
    private val append   = client.append(_)
    private val appendV2 = clientV2.appendRecords(_)
    private val delete   = client.delete(_)
    private val tomb     = client.tombstone(_)

    // Subscriptions are infinite streams and may route over a pooled transport (see sec.api.pool)
    // instead of the ops channel. Reads/appends/gossip stay on the dedicated ops channel: transient
    // calls must not ratchet a never-shrinking pool, and brief queueing is harmless for calls that end.
    private val subRead: ReadReq => Stream[F, ReadResp] =
      subscriptionTransport.fold(read)(f => f(_, summon[C]))

    private val subscriptionOpts: Opts[F] =
      opts.copy(retryOn = th => opts.retryOn(th) || th.isInstanceOf[ResubscriptionRequired])

    def subscribeToAll(
      exclusiveFrom: Option[LogPosition],
      resolveLinkTos: Boolean
    ): Stream[F, Event] =
      subscribeToAll0[F](exclusiveFrom, resolveLinkTos, subscriptionOpts)(subRead)

    def subscribeToAll(
      exclusiveFrom: Option[LogPosition],
      filterOptions: SubscriptionFilterOptions,
      resolveLinkTos: Boolean
    ): Stream[F, Either[Checkpoint, Event]] =
      subscribeToAll0[F](exclusiveFrom, filterOptions, resolveLinkTos, subscriptionOpts)(subRead)

    def subscribeToStream(
      streamId: StreamId,
      exclusiveFrom: Option[StreamPosition],
      resolveLinkTos: Boolean
    ): Stream[F, Event] =
      subscribeToStream0[F](streamId, exclusiveFrom, resolveLinkTos, subscriptionOpts)(subRead)

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

    def multiStreamAppend(
      appends: NonEmptyList[StreamAppend],
      guards: List[StreamGuard]
    ): F[MultiAppendResult] =
      multiStreamAppend0[F](appends, guards, opts)(appendV2)

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
      Streams[F, C](client, clientV2, _ => mkCtx(creds.some), opts, subscriptionTransport)

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
    val sub: Option[LogPosition] => Stream[F, Event] =
      ef =>
        f(mkReq(ef))
          .through(subscriptionAllPipe(pipeLog, opts.subscriptionConfirmationTimeout))
          .through(raiseOnServerCompletion(opName))
    val fn: Event => Option[LogPosition] = _.record.logPosition.some

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
    val sub: Option[LogPosition] => Stream[F, O] =
      ef =>
        f(mkReq(ef))
          .through(subAllFilteredPipe(pipeLog, opts.subscriptionConfirmationTimeout))
          .through(raiseOnServerCompletion(opName))
    val fn: O => Option[LogPosition] = _.fold(_.logPosition, _.record.logPosition).some

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

    val sub: Option[StreamPosition] => Stream[F, Event] =
      ef =>
        f(mkReq(ef))
          .through(subscriptionStreamPipe(pipeLog, opts.subscriptionConfirmationTimeout))
          .through(raiseOnServerCompletion(opName))

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

  private[sec] def multiStreamAppend0[F[_]: Temporal](
    appends: NonEmptyList[StreamAppend],
    guards: List[StreamGuard],
    opts: Opts[F]
  )(f: V2AppendReq => F[V2AppendResp]): F[MultiAppendResult] =

    // A stream may appear only once across a request's consistency checks; this is enforced by
    // the server (AppendRecordsRequestValidator, ordinal case-insensitive), so we send the request
    // and surface its rejection rather than replicate - and risk drifting from - that rule.
    val req = mapping.streamsV2.mkAppendRecordsRequest(appends, guards)
    opts.run(f(req), "multiStreamAppend") >>= mapping.streamsV2.mkMultiAppendResult[F](appends)

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

  /** The subscription confirmation is the one message the server is guaranteed to send immediately on every
    * subscription. Not receiving it within `confirmationTimeout` means the subscription was never established, e.g. the
    * request got in during a node shutdown or role change and the server accepted the call but will never feed it. This
    * is turned into [[ResubscriptionRequired]], which subscription retry options resume from. After the confirmation
    * the timeout no longer applies, as a quiet stream is legitimately silent.
    */
  private[sec] def subConfirmationPipe[F[_]: Temporal](
    logger: Logger[F],
    confirmationTimeout: FiniteDuration
  ): Pipe[F, ReadResp, ReadResp] = in =>

    val log: SubscriptionConfirmation => F[Unit] =
      sc => logger.debug(s"$sc received")

    val raiseTimeout: Pull[F, Nothing, Unit] =
      Pull.raiseError[F](ResubscriptionRequired(s"no subscription confirmation received within $confirmationTimeout"))

    def echo(tp: Pull.Timed[F, ReadResp]): Pull[F, ReadResp, Unit] =
      tp.uncons.flatMap {
        case Some((Right(c), next)) => Pull.output(c) >> echo(next)
        case Some((Left(_), next))  => echo(next) // stray timeout after confirmation, ignore
        case None                   => Pull.done
      }

    def awaitConfirmation(tp: Pull.Timed[F, ReadResp]): Pull[F, ReadResp, Unit] =
      tp.uncons.flatMap {
        case Some((Right(c), next)) =>
          if c.nonEmpty then Pull.eval(reqConfirmation[F](c(0)) >>= log) >> Pull.output(c.drop(1)) >> echo(next)
          else awaitConfirmation(next)
        case Some((Left(_), _)) => raiseTimeout
        case None               => Pull.done
      }

    in.pull.timed(tp => tp.timeout(confirmationTimeout) >> awaitConfirmation(tp)).stream

  private[sec] def subscriptionAllPipe[F[_]: Temporal](
    log: Logger[F],
    confirmationTimeout: FiniteDuration
  ): Pipe[F, ReadResp, Event] =
    _.filterNot(_.isCheckpoint)
      .through(subConfirmationPipe(log, confirmationTimeout))
      .through(_.filter(_.isEvent).evalMap(reqReadEvent[F]).unNone)

  private[sec] def subscriptionStreamPipe[F[_]: Temporal](
    log: Logger[F],
    confirmationTimeout: FiniteDuration
  ): Pipe[F, ReadResp, Event] =
    _.through(subConfirmationPipe(log, confirmationTimeout))
      .through(_.filter(_.isEvent).evalMap(reqReadEvent[F]).unNone)

  private[sec] def subAllFilteredPipe[F[_]: Temporal](
    log: Logger[F],
    confirmationTimeout: FiniteDuration
  ): Pipe[F, ReadResp, Either[Checkpoint, Event]] =
    // Defensive filtering to guard against ESDB emitting checkpoints
    // with `LogPosition.Exact.MaxValue`, i.e. end of stream.
    _.through(subConfirmationPipe(log, confirmationTimeout))
      .through(_.filter(_.isCheckpointOrEvent).evalMap(mkCheckpointOrEvent[F]).unNone)
      .collect {
        case r @ Right(_)                               => r
        case l @ Left(v) if v != Checkpoint.endOfStream => l
      }

  /** Subscriptions are conceptually infinite: the server never legitimately completes one. A normal completion means
    * the server dropped the subscription without an error (e.g. node shutdown or leader change during a rolling
    * restart), so it is turned into [[ResubscriptionRequired]], which subscription retry options resume from by
    * resubscribing at the last observed position. Client-side cancellation is unaffected as an interrupted stream never
    * evaluates the appended raise.
    */
  private[sec] def raiseOnServerCompletion[F[_]: RaiseThrowable, O](opName: String): Pipe[F, O, O] =
    _ ++ Stream.raiseError[F](ResubscriptionRequired(s"$opName: server completed the subscription"))

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

        val readFilter: O => F[Boolean] = out => {

          val next: T              = extractFn(out)
          val filter: T => Boolean = direction.fold(order.gt, order.lt)(next, _)

          state.get.map(_.fold(true)(filter))
        }

        def run(f: T, attempts: Int, d: FiniteDuration): Stream[F, O] =

          val readAndFilter: Stream[F, O] = streamFn(f).evalFilter(readFilter)
          val readAndUpdate: Stream[F, O] = readAndFilter.evalTap(out => state.set(extractFn(out).some))

          Stream.eval(Temporal[F].monotonic) >>= { startedAt =>
            readAndUpdate.recoverWith {

              case NonFatal(t) if o.retryOn(t) =>

                // Reset the attempt budget and delay once a reconnect has stayed healthy for at least one full
                // maxDelay window, so maxAttempts bounds *consecutive* fast failures rather than reconnects over a lifetime.
                val decide: F[(Int, FiniteDuration, T)] =
                  for
                    _       <- o.hint(t)
                    endedAt <- Temporal[F].monotonic
                    current <- state.get.map(_.getOrElse(f))
                  yield
                    if (endedAt - startedAt) >= o.retryConfig.maxDelay then (1, o.retryConfig.delay, current)
                    else (attempts, d, current)

                Stream.eval(decide) >>= { case (attempt, delay, c) =>
                  if attempt <= maxAttempts then
                    // The maxAttempts-th attempt is the last retry; it is deliberately not warned because the
                    // failure that follows it hits the exhausted branch below and logs the error instead.
                    Stream.eval(logWarn(attempt, delay, t).whenA(attempt < maxAttempts)) >>
                      run(c, attempt + 1, nextDelay(delay)).delayBy(delay)
                  else Stream.eval(logError(t)) *> Stream.raiseError[F](t)
                }
            }
          }

        run(from, 1, o.retryConfig.delay)
      }
    else streamFn(from)

//======================================================================================================================

  extension (rr: ReadResp)
    private[sec] def isEvent: Boolean             = rr.content.isEvent
    private[sec] def isCheckpoint: Boolean        = rr.content.isCheckpoint
    private[sec] def isCheckpointOrEvent: Boolean = rr.isEvent || rr.isCheckpoint
