package sec
package api

import scala.concurrent.duration._
import scala.util.control.NonFatal
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import cats.effect.concurrent.Ref
import fs2.Stream
import com.eventstore.client.streams._
import sec.core._
import sec.syntax.StreamsSyntax
import mapping.streams.outgoing._
import mapping.streams.incoming._

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
    position: Position,
    direction: ReadDirection,
    maxCount: Int,
    resolveLinkTos: Boolean,
    filter: Option[EventFilter],
    creds: Option[UserCredentials]
  ): Stream[F, Event]

  def readStream(
    streamId: StreamId,
    from: EventNumber,
    direction: ReadDirection,
    count: Int,
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

  private[sec] def metadata: StreamMeta[F]

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

    import EventFilter._

    val ctx: Option[UserCredentials] => Context = uc => {
      Context(uc.orElse(options.defaultCreds), options.connectionName)
    }

    private val mkEvents: Stream[F, ReadResp] => Stream[F, Event] =
      _.evalMap(_.event.map(mkEvent[F]).getOrElse(none[Event].pure[F])).unNone

    def subscribeToAll(
      exclusiveFrom: Option[Position],
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, Event] = {

      val subscription: Option[Position] => Stream[F, Event] =
        ef => client.read(mkSubscribeToAllReq(ef, resolveLinkTos, filter), ctx(creds)).through(mkEvents)

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
        subscribeToStream0[F](
          streamId,
          retriesWhenNotFound = 20,
          delayWhenNotFound   = 150.millis,
          client.read(mkSubscribeToStreamReq(streamId, ef, resolveLinkTos), ctx(creds)).through(mkEvents),
          subscribeToAll(None, false, prefix(ByStreamId, None, streamId.stringValue).some, creds),
          failIfNotFound
        )

      subscribeToStreamWithRetry[F](exclusiveFrom, subscription)
    }

    def readAll(
      position: Position,
      direction: ReadDirection,
      maxCount: Int,
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, Event] =
      client.read(mkReadAllReq(position, direction, maxCount, resolveLinkTos, filter), ctx(creds)).through(mkEvents)

    def readStream(
      streamId: StreamId,
      from: EventNumber,
      direction: ReadDirection,
      count: Int,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] =
      client.read(mkReadStreamReq(streamId, from, direction, count, resolveLinkTos), ctx(creds)).through(mkEvents)

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

    private[sec] val metadata: StreamMeta[F] = StreamMeta[F](this)
  }

//======================================================================================================================

  private[sec] def subscribeToStream0[F[_]: ConcurrentEffect: Timer](
    streamId: StreamId,
    retriesWhenNotFound: Int,
    delayWhenNotFound: FiniteDuration,
    source: Stream[F, Event],
    globalSource: Stream[F, Event],
    failIfNotFound: Boolean
  ): Stream[F, Event] = {

    def allSubscription: Stream[F, Event] =
      globalSource.filter(_.streamId === streamId)

    def subscribeWithRetry(retriesLeft: Int): Stream[F, Event] =
      source.recoverWith {
        case _: StreamNotFound =>
          if (retriesLeft > 0) subscribeWithRetry(retriesLeft - 1).delayBy(delayWhenNotFound) else Stream.never
      }

    val waitForSourceOrGlobal: Stream[F, Event] =
      subscribeWithRetry(retriesWhenNotFound).take(1).mergeHaltBoth(allSubscription.take(1))

    source.recoverWith {
      case _: StreamNotFound if !failIfNotFound => waitForSourceOrGlobal >> source
    }

  }

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
