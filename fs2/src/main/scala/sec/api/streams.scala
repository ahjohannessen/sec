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

import java.util.{UUID => JUUID}
import scala.concurrent.duration._
import scala.util.control.NonFatal
import cats._
import cats.data._
import cats.effect._
import cats.syntax.all._
import com.eventstore.client.streams._
import fs2._
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.Logger
import sec.api.exceptions.WrongExpectedVersion
import sec.api.mapping.streams.incoming._
import sec.api.mapping.streams.outgoing._

/** API for interacting with streams in EventStoreDB.
  *
  * 122Main operations122
  *
  *   - subscribing to the global stream or an individual stream.
  *   - reading from the global stream or an individual stream.
  *   - appending event data to an existing stream or creating a new stream.
  *   - deleting events from a stream.
  *
  * @tparam F
  *   the effect type in which [[Streams]] operates.
  */
trait Streams[F[_]] {

  /** Subscribes to the global stream, [[StreamId.All]].
    *
    * @param exclusiveFrom
    *   position to start from. Use [[None]] to subscribe from the beginning.
    * @param resolveLinkTos
    *   whether to resolve [[EventType.LinkTo]] events automatically.
    * @return
    *   a [[Stream]] that emits [[AllEvent]] values.
    */
  def subscribeToAll(
    exclusiveFrom: Option[LogPosition],
    resolveLinkTos: Boolean
  ): Stream[F, AllEvent]

  /** Subscribes to the global stream, [[StreamId.All]] using a subscription filter.
    *
    * @param exclusiveFrom
    *   log position to start from. Use [[None]] to subscribe from the beginning.
    * @param filterOptions
    *   to use when subscribing - See [[sec.api.SubscriptionFilterOptions]].
    * @param resolveLinkTos
    *   whether to resolve [[EventType.LinkTo]] events automatically.
    * @return
    *   a [[Stream]] that emits either [[Checkpoint]] or [[AllEvent]] values. How frequent [[Checkpoint]] is emitted
    *   depends on `filterOptions`.
    */
  def subscribeToAll(
    exclusiveFrom: Option[LogPosition],
    filterOptions: SubscriptionFilterOptions,
    resolveLinkTos: Boolean
  ): Stream[F, Either[Checkpoint, AllEvent]]

  /** Subscribes to an individual stream.
    *
    * @param streamId
    *   the id of the stream to subscribe to.
    * @param exclusiveFrom
    *   stream position to start from. Use [[None]] to subscribe from the beginning.
    * @param resolveLinkTos
    *   whether to resolve [[EventType.LinkTo]] events automatically.
    * @return
    *   a [[Stream]] that emits [[StreamEvent]] values.
    */
  def subscribeToStream(
    streamId: StreamId,
    exclusiveFrom: Option[StreamPosition],
    resolveLinkTos: Boolean
  ): Stream[F, StreamEvent]

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
    *   a [[Stream]] that emits [[AllEvent]] values.
    */
  def readAll(
    from: LogPosition,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean
  ): Stream[F, AllEvent]

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
    *   a [[Stream]] that emits [[StreamEvent]] values.
    */
  def readStream(
    streamId: StreamId,
    from: StreamPosition,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean
  ): Stream[F, StreamEvent]

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
    *   stream of event data to be appended to the event stream. See [[EventData]].
    */
  def appendToStream(
    streamId: StreamId,
    expectedState: StreamState,
    data: Stream[F, EventData]
  ): F[WriteResult]

  /** Deletes a stream and returns [[DeleteResult]] with current log position after a successful operation. Failure to
    * fulfill the expected stated is manifested by raising [[sec.api.exceptions.WrongExpectedState]].
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
    * Failure to fulfill the expected stated is manifested by raising [[sec.api.exceptions.WrongExpectedState]].
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

}

object Streams {

//======================================================================================================================

  private[sec] def apply[F[_]: Async, C](
    client: StreamsFs2Grpc[F, C],
    mkCtx: Option[UserCredentials] => C,
    maxBatchSizeBytes: Int,
    batchAppender: BatchAppender[F],
    opts: Opts[F]
  ): Streams[F] = new Streams[F] {

    private val ctx = mkCtx(None)

    def subscribeToAll(
      exclusiveFrom: Option[LogPosition],
      resolveLinkTos: Boolean
    ): Stream[F, AllEvent] =
      subscribeToAll0[F](exclusiveFrom, resolveLinkTos, opts)(client.read(_, ctx))

    def subscribeToAll(
      exclusiveFrom: Option[LogPosition],
      filterOptions: SubscriptionFilterOptions,
      resolveLinkTos: Boolean
    ): Stream[F, Either[Checkpoint, AllEvent]] =
      subscribeToAll0[F](exclusiveFrom, filterOptions, resolveLinkTos, opts)(client.read(_, ctx))

    def subscribeToStream(
      streamId: StreamId,
      exclusiveFrom: Option[StreamPosition],
      resolveLinkTos: Boolean
    ): Stream[F, StreamEvent] =
      subscribeToStream0[F](streamId, exclusiveFrom, resolveLinkTos, opts)(client.read(_, ctx))

    def readAll(
      from: LogPosition,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean
    ): Stream[F, AllEvent] =
      readAll0[F](from, direction, maxCount, resolveLinkTos, opts)(client.read(_, ctx))

    def readStream(
      streamId: StreamId,
      from: StreamPosition,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean
    ): Stream[F, StreamEvent] =
      readStream0[F](streamId, from, direction, maxCount, resolveLinkTos, opts)(client.read(_, ctx))

    def appendToStream(
      streamId: StreamId,
      expectedState: StreamState,
      data: NonEmptyList[EventData]
    ): F[WriteResult] =
      appendToStream(streamId, expectedState, Stream.emits(data.toList))
//      appendToStream0[F](streamId, expectedState, data, opts)(client.append(_, ctx))

    def appendToStream(
      streamId: StreamId,
      expectedState: StreamState,
      data: Stream[F, EventData]
    ): F[WriteResult] =
      appendToStream0[F](
        streamId,
        expectedState,
        data,
        Sync[F].delay(JUUID.randomUUID()),
        maxBatchSizeBytes,
        opts
      )(batchAppender)

    def delete(
      streamId: StreamId,
      expectedState: StreamState
    ): F[DeleteResult] =
      delete0[F](streamId, expectedState, opts)(client.delete(_, ctx))

    def tombstone(
      streamId: StreamId,
      expectedState: StreamState
    ): F[TombstoneResult] =
      tombstone0[F](streamId, expectedState, opts)(client.tombstone(_, ctx))

    def withCredentials(
      creds: UserCredentials
    ): Streams[F] =
      Streams[F, C](client, _ => mkCtx(creds.some), maxBatchSizeBytes, batchAppender, opts)
  }

//======================================================================================================================

  private[sec] def subscribeToAll0[F[_]: Temporal](
    exclusiveFrom: Option[LogPosition],
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, AllEvent] = {

    val opName: String                                  = "subscribeToAll"
    val pipeLog: Logger[F]                              = opts.log.withModifiedString(m => s"$opName: $m")
    val mkReq: Option[LogPosition] => ReadReq           = mkSubscribeToAllReq(_, resolveLinkTos, None)
    val sub: Option[LogPosition] => Stream[F, AllEvent] = ef => f(mkReq(ef)).through(subscriptionAllPipe(pipeLog))
    val fn: AllEvent => Option[LogPosition]             = _.record.logPosition.some

    withRetry(exclusiveFrom, sub, fn, opts, opName, Direction.Forwards)

  }

  private[sec] def subscribeToAll0[F[_]: Temporal](
    exclusiveFrom: Option[LogPosition],
    filterOptions: SubscriptionFilterOptions,
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, Either[Checkpoint, AllEvent]] = {

    type O = Either[Checkpoint, AllEvent]

    val opName: String                           = "subscribeToAllWithFilter"
    val pipeLog: Logger[F]                       = opts.log.withModifiedString(m => s"$opName: $m")
    val mkReq: Option[LogPosition] => ReadReq    = mkSubscribeToAllReq(_, resolveLinkTos, filterOptions.some)
    val sub: Option[LogPosition] => Stream[F, O] = ef => f(mkReq(ef)).through(subAllFilteredPipe(pipeLog))
    val fn: O => Option[LogPosition]             = _.fold(_.logPosition, _.record.logPosition).some

    withRetry(exclusiveFrom, sub, fn, opts, opName, Direction.Forwards)
  }

  private[sec] def subscribeToStream0[F[_]: Temporal](
    streamId: StreamId,
    exclusiveFrom: Option[StreamPosition],
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, StreamEvent] = {

    val opName: String                           = "subscribeToStream"
    val pipeLog: Logger[F]                       = opts.log.withModifiedString(m => s"$opName[${streamId.render}]: $m")
    val mkReq: Option[StreamPosition] => ReadReq = mkSubscribeToStreamReq(streamId, _, resolveLinkTos)

    val sub: Option[StreamPosition] => Stream[F, StreamEvent] = ef =>
      f(mkReq(ef)).through(subscriptionStreamPipe(pipeLog))

    val fn: StreamEvent => Option[StreamPosition] = _.record.streamPosition.some

    withRetry(exclusiveFrom, sub, fn, opts, opName, Direction.Forwards)
  }

  private[sec] def readAll0[F[_]: Temporal](
    from: LogPosition,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, AllEvent] = {

    val mkReq: LogPosition => ReadReq            = mkReadAllReq(_, direction, maxCount, resolveLinkTos)
    val read: LogPosition => Stream[F, AllEvent] = p => f(mkReq(p)).through(readAllEventPipe).take(maxCount)
    val fn: AllEvent => LogPosition              = _.record.logPosition

    if (maxCount > 0) withRetry(from, read, fn, opts, "readAll", direction) else Stream.empty
  }

  private[sec] def readStream0[F[_]: Temporal](
    streamId: StreamId,
    from: StreamPosition,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean,
    opts: Opts[F]
  )(f: ReadReq => Stream[F, ReadResp]): Stream[F, StreamEvent] = {

    val valid: Boolean = direction.fold(from =!= StreamPosition.End, true)

    val mkReq: StreamPosition => ReadReq =
      mkReadStreamReq(streamId, _, direction, maxCount, resolveLinkTos)

    val read: StreamPosition => Stream[F, StreamEvent] = e =>
      f(mkReq(e)).through(streamNotFoundPipe).through(readStreamEventPipe)

    val fn: StreamEvent => StreamPosition = _.record.streamPosition

    if (valid && maxCount > 0) withRetry(from, read, fn, opts, "readStream", direction) else Stream.empty
  }

  private[sec] def appendToStream0[F[_]: Temporal](
    streamId: StreamId,
    expectedState: StreamState,
    eventsNel: NonEmptyList[EventData],
    opts: Opts[F]
  )(f: Stream[F, AppendReq] => F[AppendResp]): F[WriteResult] = {

    val header: AppendReq        = mkAppendHeaderReq(streamId, expectedState)
    val events: List[AppendReq]  = mkAppendProposalsReq(eventsNel).toList
    val operation: F[AppendResp] = f(Stream.emit(header) ++ Stream.emits(events))

    opts.run(operation, "appendToStream") >>= { ar => mkWriteResult[F](streamId, ar) }
  }

  private[sec] def appendToStream0[F[_]: Async](
    streamId: StreamId,
    expectedState: StreamState,
    events: Stream[F, EventData],
    mkJuuid: F[JUUID],
    maxBatchSizeInBytes: Int,
    opts: Opts[F]
  )(appender: BatchAppender[F]): F[WriteResult] = {

    mkJuuid >>= { uuid =>

      def run0(
        first: Boolean,
        data: Stream[F, BatchAppendReq.ProposedMessage],
        acc: List[BatchAppendReq.ProposedMessage],
        accSize: Int
      ): Pull[F, BatchAppendReq, Unit] = {

        def mkRequest(isFinal: Boolean): BatchAppendReq =
          if (first)
            mkProposalsReq(uuid, acc.reverse, isFinal)
              .withOptions(mkBatchAppendHeader(streamId, expectedState, None))
          else
            mkProposalsReq(uuid, acc.reverse, isFinal)

        data.pull.uncons1.flatMap {

          case Some((h, t)) =>
            val size     = h.serializedSize
            val nextSize = accSize + size

            if (nextSize < maxBatchSizeInBytes)
              run0(first, t, h :: acc, nextSize)
            else
              Pull.output1(mkRequest(isFinal = false)) >> run0(false, t, List(h), size)

          case None =>
            Pull.output1(mkRequest(isFinal = true)) >> Pull.done
        }
      }

      val writer: Stream[F, BatchAppendReq] =
        run0(first = true, events.map(mkBatchAppendProposal), Nil, 0).stream

      opts.run(appender.append(uuid, writer), "appendToStream") >>= mkBatchWriteResult[F]

    }

  }

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

  private[sec] def readAllEventPipe[F[_]: MonadThrow]: Pipe[F, ReadResp, AllEvent] =
    _.evalMap(reqReadAll[F]).unNone

  private[sec] def readStreamEventPipe[F[_]: MonadThrow]: Pipe[F, ReadResp, StreamEvent] =
    _.evalMap(reqReadStream[F]).unNone

  private[sec] def streamNotFoundPipe[F[_]: MonadThrow]: Pipe[F, ReadResp, ReadResp] =
    _.evalMap(failStreamNotFound[F])

  private[sec] def subConfirmationPipe[F[_]: MonadThrow](logger: Logger[F]): Pipe[F, ReadResp, ReadResp] = in => {

    val log: SubscriptionConfirmation => F[Unit] =
      sc => logger.debug(s"$sc received")

    val initialPull = in.pull.uncons1.flatMap {
      case Some((h, t)) => Pull.eval(reqConfirmation[F](h) >>= log) >> t.pull.echo
      case None         => Pull.done
    }

    initialPull.stream
  }

  private[sec] def subscriptionAllPipe[F[_]: MonadThrow](
    log: Logger[F]
  ): Pipe[F, ReadResp, AllEvent] =
    _.through(subConfirmationPipe(log)).through(readAllEventPipe)

  private[sec] def subscriptionStreamPipe[F[_]: MonadThrow](
    log: Logger[F]
  ): Pipe[F, ReadResp, StreamEvent] =
    _.through(subConfirmationPipe(log)).through(readStreamEventPipe)

  private[sec] def subAllFilteredPipe[F[_]: MonadThrow](
    log: Logger[F]
  ): Pipe[F, ReadResp, Either[Checkpoint, AllEvent]] =
    _.through(subConfirmationPipe(log)).through(_.evalMap(mkCheckpointOrEvent[F]).unNone)

  private[sec] def withRetry[F[_]: Temporal, T: Order, O](
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

  private[sec] trait BatchAppender[F[_]] {
    def append(cid: JUUID, source: Stream[F, BatchAppendReq]): F[BatchAppendResp]
  }

  private[sec] object BatchAppender {

    import cats.effect.implicits._
    import cats.effect.std.Queue
    import mapping.implicits._
    import mapping.shared.mkJuuid

    def apply[F[_]](handle: Stream[F, BatchAppendReq] => Stream[F, BatchAppendResp])(implicit
      F: Async[F]): Resource[F, BatchAppender[F]] = {

      def extractId(x: BatchAppendResp): F[JUUID] =
        x.correlationId.require[F]("correlationId") >>= mkJuuid[F]

      def handleIncoming(x: BatchAppendResp, outstanding: Ref[F, Map[JUUID, Deferred[F, BatchAppendResp]]]) =
        extractId(x) >>= { id =>
          outstanding.modify(m => (m - id, m.get(id))).flatMap(_.fold(F.unit)(_.complete(x).void))
        }

      for {
        outgoing      <- Resource.make(Queue.unbounded[F, Option[BatchAppendReq]])(_.offer(None))
        outgoingStream = Stream.fromQueueNoneTerminated(outgoing)
        outstanding   <- Resource.eval(Ref.of[F, Map[JUUID, Deferred[F, BatchAppendResp]]](Map.empty))
        ingest         = handle(outgoingStream).evalMap(handleIncoming(_, outstanding))
        _             <- ingest.compile.drain.background

      } yield
        BatchAppender[F](
          (ba: BatchAppendReq) => outgoing.offer(ba.some),
          outstanding
        )

    }

    def apply[F[_]: Async](
      outgoing: BatchAppendReq => F[Unit],
      incoming: Ref[F, Map[JUUID, Deferred[F, BatchAppendResp]]]
    ): BatchAppender[F] = new BatchAppender[F] {

      def append(cid: JUUID, source: Stream[F, BatchAppendReq]): F[BatchAppendResp] =
        for {
          p <- Deferred[F, BatchAppendResp]
          _ <- incoming.update(_ + (cid -> p))
          _ <-
            source
              .evalTap(outgoing)
              .evalTap(ba =>
                Async[F].delay(println(s"Writing ${ba.proposedMessages.size} events ${ba.proposedMessages.foldLeft(0)(
                  (b, m) => m.serializedSize + b) / ba.proposedMessages.size} bytes average")))
              .compile
              .drain
              .onError { case e =>
                incoming.update(_ - cid)
              }
          pf <- p.get
        } yield pf
    }

  }

}
