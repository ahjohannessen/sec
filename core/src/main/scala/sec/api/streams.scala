package sec
package api

import scala.concurrent.duration._
import scala.util.control.NonFatal
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import cats.effect.concurrent.Ref
import fs2.{Pull, Stream}
import com.eventstore.client.streams._
import sec.core._
import sec.syntax.StreamsSyntax
import mapping.streams.outgoing._
import mapping.streams.incoming._
import mapping.implicits._

trait Streams[F[_]] {

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean,
    filter: Option[EventFilter],
    creds: Option[UserCredentials]
  ): Stream[F, Event]

  def subscribeToStream(
    streamId: StreamId,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean,
    failIfNotFound: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, Event]

  def readAll(
    from: Position,
    direction: Direction,
    maxCount: Long,
    resolveLinkTos: Boolean,
    filter: Option[EventFilter],
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

  def softDelete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[Streams.DeleteResult]

  def hardDelete(
    streamId: StreamId,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[Streams.DeleteResult]

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

  private[sec] def apply[F[_]: ConcurrentEffect: Timer](
    client: StreamsFs2Grpc[F, Context],
    options: Options
  ): Streams[F] = new Impl[F](client, options)

  private[sec] final class Impl[F[_]: ConcurrentEffect: Timer](
    val client: StreamsFs2Grpc[F, Context],
    val options: Options
  ) extends Streams[F] {

    val ctx: Option[UserCredentials] => Context = uc => {
      Context(uc.orElse(options.defaultCreds), options.connectionName)
    }

    private val readPipe: Stream[F, ReadResp] => Stream[F, Event] =
      _.evalMap(_.content.event.require[F]("ReadEvent expected!").flatMap(mkEvent[F])).unNone

    private val subscriptionPipe: Stream[F, ReadResp] => Stream[F, Event] = in => {

      val extractConfirmation: ReadResp => F[String] =
        _.content.confirmation.map(_.subscriptionId).require[F]("SubscriptionConfirmation expected!")

      val initialPull = in.pull.uncons1.flatMap {
        case Some((hd, tail)) => Pull.eval(extractConfirmation(hd)) >> tail.pull.echo
        case None             => Pull.done
      }

      initialPull.stream.through(readPipe)
    }

    ///

    def subscribeToAll(
      exclusiveFrom: Option[Position],
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, Event] = {

      val subscription: Option[Position] => Stream[F, Event] = ef =>
        client.read(mkSubscribeToAllReq(ef, resolveLinkTos, filter), ctx(creds)).through(subscriptionPipe)

      subscribeToAllWithRetry[F](exclusiveFrom, subscription)
    }

    def subscribeToStream(
      streamId: StreamId,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean,
      failIfNotFound: Boolean,
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
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, Event] = {

      def validateFilter: F[Unit] = filter.flatMap(_.maxSearchWindow.filterNot(_ > maxCount)).fold(().pure[F]) { msw =>
        ValidationError(s"maxSearchWindow: $msw must be > maxCount: $maxCount").raiseError[F, Unit]
      }

      if (maxCount > 0) {

        Stream.eval(validateFilter) >>
          client.read(mkReadAllReq(from, direction, maxCount, resolveLinkTos, filter), ctx(creds)).through(readPipe)

      } else Stream.empty
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

      if (valid && maxCount > 0) {

        client.read(mkReadStreamReq(streamId, from, direction, maxCount, resolveLinkTos), ctx(creds)).through(readPipe)

      } else Stream.empty
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
      ) >>= mkWriteResult[F]

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

  private[sec] def subscribeToAllWithRetry[F[_]: Sync: Timer](
    exclusiveFrom: Option[Position],
    subscription: Option[Position] => Stream[F, Event]
  ): Stream[F, Event] =
    subscribeWithRetry[F, Position](exclusiveFrom, subscription, _.record.position)

  private[sec] def subscribeToStreamWithRetry[F[_]: Sync: Timer](
    exclusiveFrom: Option[EventNumber],
    subscription: Option[EventNumber] => Stream[F, Event]
  ): Stream[F, Event] =
    subscribeWithRetry[F, EventNumber](exclusiveFrom, subscription, _.record.number)

  private[sec] def subscribeWithRetry[F[_]: Sync: Timer, T](
    exclusiveFrom: Option[T],
    subscription: Option[T] => Stream[F, Event],
    fn: Event => T,
    delay: FiniteDuration = 200.millis,
    nextDelay: FiniteDuration => FiniteDuration = identity,
    maxAttempts: Int = 100,
    retriable: Throwable => Boolean = _.isInstanceOf[ServerUnavailable]
  ): Stream[F, Event] = {

    def eval(ef: Option[T], attempts: Int, d: FiniteDuration): Stream[F, Event] =
      Stream.eval(Ref.of[F, Option[T]](ef)).flatMap { r =>
        subscription(ef).changesBy(_.record.position).evalTap(e => r.set(fn(e).some)).recoverWith {
          case NonFatal(t) if retriable(t) && attempts < maxAttempts =>
            Stream.eval(r.get).flatMap(n => eval(n, attempts + 1, nextDelay(d)).delayBy(d))
        }
      }

    eval(exclusiveFrom, 0, delay)
  }

}
