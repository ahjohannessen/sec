package sec
package api

import scala.concurrent.duration._
import scala.util.control.NonFatal
import cats.Eq
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import cats.effect.concurrent.Ref
import fs2.{Pipe, Pull, Stream}
import com.eventstore.client.streams._
import sec.core._
import sec.syntax.StreamsSyntax
import mapping.shared._
import mapping.streams.outgoing._
import mapping.streams.incoming._
import mapping.implicits._

trait Streams[F[_]] {

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, Event]

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    filter: EventFilter,
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, Either[Position, Event]]

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

  def softDelete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[DeleteResult]

  def hardDelete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[DeleteResult]

  private[sec] def metadata: MetaStreams[F]

}

object Streams {

  implicit def syntaxForStreams[F[_]](s: Streams[F]): StreamsSyntax[F] = new StreamsSyntax[F](s)

  ///

  final case class WriteResult(currentRevision: EventNumber.Exact)
  final case class DeleteResult(position: Position.Exact)

//======================================================================================================================
//                                                  Impl
//======================================================================================================================

  private[sec] def apply[F[_]: Sync: Timer](
    client: StreamsFs2Grpc[F, Context],
    options: Options
  ): Streams[F] = new Impl[F](client, options)

  final private[sec] class Impl[F[_]: Timer](
    val client: StreamsFs2Grpc[F, Context],
    val options: Options
  )(implicit F: Sync[F])
    extends Streams[F] {

    val ctx: Option[UserCredentials] => Context = uc => {
      Context(options.connectionName, uc.orElse(options.defaultCreds), options.nodePreference.isLeader)
    }

    private val readEventPipe: Stream[F, ReadResp] => Stream[F, Event] =
      _.evalMap(_.content.event.require[F]("ReadEvent expected!").flatMap(mkEvent[F])).unNone

    private val failStreamNotFound: Pipe[F, ReadResp, ReadResp] =
      _.evalMap(r =>
        r.content.streamNotFound.fold(r.pure[F])(
          _.streamIdentifier
            .require[F]("StreamIdentifer expected!")
            .flatMap(_.utf8[F].map(StreamNotFound(_)))
            .flatMap(F.raiseError)
        ))

    private val subConfirmationPipe: Stream[F, ReadResp] => Stream[F, ReadResp] = in => {

      val extractConfirmation: ReadResp => F[String] =
        _.content.confirmation.map(_.subscriptionId).require[F]("SubscriptionConfirmation expected!")

      val initialPull = in.pull.uncons1.flatMap {
        case Some((hd, tail)) => Pull.eval(extractConfirmation(hd)) >> tail.pull.echo
        case None             => Pull.done
      }

      initialPull.stream
    }

    private val subscriptionPipe: Stream[F, ReadResp] => Stream[F, Event] =
      _.through(subConfirmationPipe).through(readEventPipe)

    private val subAllFilteredPipe: Stream[F, ReadResp] => Stream[F, Either[Position, Event]] = {

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

//======================================================================================================================

    def subscribeToAll(
      exclusiveFrom: Option[Position],
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] = {

      val subscription: Option[Position] => Stream[F, Event] = ef =>
        client.read(mkSubscribeToAllReq(ef, resolveLinkTos, None), ctx(creds)).through(subscriptionPipe)

      subscribeToAllWithRetry[F, Event](exclusiveFrom, subscription, _.record.position)
    }

    ///

    def subscribeToAll(
      exclusiveFrom: Option[Position],
      filter: EventFilter,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Either[Position, Event]] = {

      val subscription: Option[Position] => Stream[F, Either[Position, Event]] = ef =>
        client.read(mkSubscribeToAllReq(ef, resolveLinkTos, filter.some), ctx(creds)).through(subAllFilteredPipe)

      val posFn: Either[Position, Event] => Position = _.fold(identity, _.record.position)

      subscribeToAllWithRetry[F, Either[Position, Event]](exclusiveFrom, subscription, posFn)
    }

    def subscribeToStream(
      streamId: StreamId,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] = {

      val subscription: Option[EventNumber] => Stream[F, Event] = ef =>
        client.read(mkSubscribeToStreamReq(streamId, ef, resolveLinkTos), ctx(creds)).through(subscriptionPipe)

      subscribeToStreamWithRetry[F](exclusiveFrom, subscription)
    }

    def readAll(
      from: Position,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] = {

      if (maxCount > 0)
        client.read(mkReadAllReq(from, direction, maxCount, resolveLinkTos), ctx(creds)).through(readEventPipe)
      else Stream.empty
    }

    def readStream(
      streamId: StreamId,
      from: EventNumber,
      direction: Direction,
      maxCount: Long,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] = {

      val valid = direction.fold(from =!= EventNumber.End, true)
      def req   = mkReadStreamReq(streamId, from, direction, maxCount, resolveLinkTos)

      if (valid && maxCount > 0)
        client.read(req, ctx(creds)).through(failStreamNotFound).through(readEventPipe)
      else Stream.empty
    }

    def appendToStream(
      streamId: StreamId,
      expectedRevision: StreamRevision,
      events: NonEmptyList[EventData],
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      client.append(
        Stream.emit(mkAppendHeaderReq(streamId, expectedRevision)) ++ Stream.emits(mkAppendProposalsReq(events).toList),
        ctx(creds)
      ) >>= { ar => mkWriteResult[F](streamId, ar) }

    def softDelete(
      streamId: StreamId,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[DeleteResult] =
      client.delete(mkSoftDeleteReq(streamId, expectedRevision), ctx(creds)) >>= mkDeleteResult[F]

    def hardDelete(
      streamId: StreamId,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[DeleteResult] =
      client.tombstone(mkHardDeleteReq(streamId, expectedRevision), ctx(creds)) >>= mkDeleteResult[F]

    private[sec] val metadata: MetaStreams[F] = MetaStreams[F](this)
  }

//======================================================================================================================

  private[sec] def subscribeToAllWithRetry[F[_]: Sync: Timer, O](
    exclusiveFrom: Option[Position],
    subscription: Option[Position] => Stream[F, O],
    posFn: O => Position
  ): Stream[F, O] =
    subscribeWithRetry[F, Position, O](exclusiveFrom, subscription, posFn)

  private[sec] def subscribeToStreamWithRetry[F[_]: Sync: Timer](
    exclusiveFrom: Option[EventNumber],
    subscription: Option[EventNumber] => Stream[F, Event]
  ): Stream[F, Event] =
    subscribeWithRetry[F, EventNumber, Event](exclusiveFrom, subscription, _.record.number)

  private[sec] def subscribeWithRetry[F[_]: Sync: Timer, T: Eq, O](
    exclusiveFrom: Option[T],
    subscription: Option[T] => Stream[F, O],
    fn: O => T,
    delay: FiniteDuration = 200.millis,
    nextDelay: FiniteDuration => FiniteDuration = identity,
    maxAttempts: Int = 100,
    retriable: Throwable => Boolean = _.isInstanceOf[ServerUnavailable]
  ): Stream[F, O] = {

    def eval(ef: Option[T], attempts: Int, d: FiniteDuration): Stream[F, O] =
      Stream.eval(Ref.of[F, Option[T]](ef)).flatMap { r =>
        subscription(ef).changesBy(fn).evalTap(o => r.set(fn(o).some)).recoverWith {
          case NonFatal(t) if retriable(t) && attempts < maxAttempts =>
            Stream.eval(r.get).flatMap(n => eval(n, attempts + 1, nextDelay(d)).delayBy(d))
        }
      }

    eval(exclusiveFrom, 0, delay)
  }

}
