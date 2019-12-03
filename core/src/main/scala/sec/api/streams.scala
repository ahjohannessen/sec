package sec
package api

import cats.data.NonEmptyList
import cats.effect.Sync
import cats.implicits._
import io.grpc.Metadata
import fs2.Stream
import com.eventstore.client.streams._
import sec.core._
import sec.syntax.StreamsSyntax
import mapping.streams.outgoing._
import mapping.streams.incoming._
import grpc._

trait Streams[F[_]] {

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean,
    filter: Option[EventFilter],
    creds: Option[UserCredentials]
  ): Stream[F, Event]

  def subscribeToStream(
    stream: String,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean,
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
    stream: String,
    direction: ReadDirection,
    from: EventNumber,
    count: Int,
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, Event]

  def appendToStream(
    stream: String,
    expectedRevision: StreamRevision,
    events: NonEmptyList[EventData],
    creds: Option[UserCredentials]
  ): F[Streams.WriteResult]

  def softDelete(
    stream: String,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[Streams.DeleteResult]

  def hardDelete(
    stream: String,
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

  private[sec] def apply[F[_]: Sync](
    client: StreamsFs2Grpc[F, Metadata],
    options: Options
  ): Streams[F] = new Impl[F](client, options)

  private[sec] final class Impl[F[_]](
    val client: StreamsFs2Grpc[F, Metadata],
    val options: Options
  )(implicit F: Sync[F])
    extends Streams[F] {

    val auth: Option[UserCredentials] => Metadata =
      _.orElse(options.defaultCreds).map(_.toMetadata).getOrElse(new Metadata())

    private val mkEvents: Stream[F, ReadResp] => Stream[F, Event] =
      _.evalMap(_.event.map(mkEvent[F]).getOrElse(none[Event].pure[F])).unNone

    def subscribeToAll(
      exclusiveFrom: Option[Position],
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, Event] =
      client.read(mkSubscribeToAllReq(exclusiveFrom, resolveLinkTos, filter), auth(creds)).through(mkEvents)

    def subscribeToStream(
      stream: String,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] =
      client.read(mkSubscribeToStreamReq(stream, exclusiveFrom, resolveLinkTos), auth(creds)).through(mkEvents)

    def readAll(
      position: Position,
      direction: ReadDirection,
      maxCount: Int,
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, Event] =
      client.read(mkReadAllReq(position, direction, maxCount, resolveLinkTos, filter), auth(creds)).through(mkEvents)

    def readStream(
      stream: String,
      direction: ReadDirection,
      from: EventNumber,
      count: Int,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] =
      client.read(mkReadStreamReq(stream, direction, from, count, resolveLinkTos), auth(creds)).through(mkEvents)

    def appendToStream(
      stream: String,
      expectedRevision: StreamRevision,
      events: NonEmptyList[EventData],
      creds: Option[UserCredentials]
    ): F[WriteResult] = {
      val header    = mkAppendHeaderReq(stream, expectedRevision)
      val proposals = mkAppendProposalsReq(events).toList
      val request   = Stream.emit(header) ++ Stream.emits(proposals)
      client.append(request, auth(creds)) >>= mkWriteResult[F]
    }

    def softDelete(
      stream: String,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[DeleteResult] =
      client.delete(mkSoftDeleteReq(stream, expectedRevision), auth(creds)) >>= mkDeleteResult[F]

    def hardDelete(
      stream: String,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[DeleteResult] =
      client.tombstone(mkHardDeleteReq(stream, expectedRevision), auth(creds)) >>= mkDeleteResult[F]

    private[sec] val metadata: StreamMeta[F] = StreamMeta[F](this)
  }

}
