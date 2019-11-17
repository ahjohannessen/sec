package sec

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import io.grpc.Metadata
import fs2.Stream
import io.grpc.StatusRuntimeException
import com.eventstore.client.streams._
import sec.core._
import sec.grpc._
import sec.format._

trait EsClient[F[_]] {

  def subscribeToAll(
    exclusiveFrom: Option[Position],
    resolveLinkTos: Boolean,
    filter: Option[EventFilter],
    creds: Option[UserCredentials]
  ): Stream[F, com.eventstore.client.streams.ReadResp] // temp

  def subscribeToStream(
    stream: String,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, com.eventstore.client.streams.ReadResp] // temp

  def readAll(
    position: Position,
    direction: ReadDirection,
    maxCount: Int,
    resolveLinkTos: Boolean,
    filter: Option[EventFilter],
    creds: Option[UserCredentials]
  ): Stream[F, com.eventstore.client.streams.ReadResp] // temp

  def readStream(
    stream: String,
    direction: ReadDirection,
    from: EventNumber,
    count: Int,
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, com.eventstore.client.streams.ReadResp] // temp

  def appendToStream(
    stream: String,
    expectedRevision: StreamRevision,
    events: NonEmptyList[EventData],
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def softDelete(
    stream: String,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[com.eventstore.client.streams.DeleteResp] // temp

  def tombstone(
    stream: String,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[com.eventstore.client.streams.TombstoneResp] // temp

}

object EsClient {

  implicit def syntaxForESClient[F[_]](esc: EsClient[F]): sec.syntax.EsClientSyntax[F] =
    new sec.syntax.EsClientSyntax[F](esc)

  val extractEsException: PartialFunction[Throwable, Throwable] = {
    case e: StatusRuntimeException => convertToEs(e).getOrElse(e)
  }

  ///

  ///

  final case class Config(
    userCredentials: UserCredentials
  )

  def apply[F[_]: ConcurrentEffect](client: Streams[F, Metadata], cfg: Config): EsClient[F] = new EsClient[F] {

    val authFallback: Metadata                    = cfg.userCredentials.toMetadata
    val auth: Option[UserCredentials] => Metadata = _.fold(authFallback)(_.toMetadata)

    def subscribeToAll(
      exclusiveFrom: Option[Position],
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, ReadResp] =
      streams.subscribeToAll[F](client.read(_, auth(creds)))(exclusiveFrom, resolveLinkTos)

    def subscribeToStream(
      stream: String,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, ReadResp] =
      streams.subscribeToStream(client.read(_, auth(creds)))(stream, exclusiveFrom, resolveLinkTos)

    def readAll(
      position: Position,
      direction: ReadDirection,
      maxCount: Int,
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, ReadResp] =
      streams.readAll[F](client.read(_, auth(creds)))(position, direction, maxCount, resolveLinkTos)

    def readStream(
      stream: String,
      direction: ReadDirection,
      from: EventNumber,
      count: Int,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, ReadResp] =
      streams.readStream[F](client.read(_, auth(creds)))(stream, direction, from, count, resolveLinkTos)

    def appendToStream(
      stream: String,
      expectedRevision: StreamRevision,
      events: NonEmptyList[EventData],
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      streams.appendToStream[F](client.append(_, auth(creds)))(stream, expectedRevision, events.toList)

    def softDelete(
      stream: String,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[DeleteResp] =
      streams.softDelete[F](client.delete(_, auth(creds)))(stream, expectedRevision)

    def tombstone(
      stream: String,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[TombstoneResp] =
      streams.tombstone[F](client.tombstone(_, auth(creds)))(stream, expectedRevision)
  }

  // format: off

  private[sec] object streams {
    
    import ReadReq.Options.AllOptions
    import ReadReq.Options.StreamOptions
    import sec.grpc.mapping.Streams._

    ///
    
    def subscribeToAll[F[_]: Sync](readFn: ReadReq => Stream[F, ReadResp])(
      exclusiveFrom: Option[Position],
      resolveLinkTos: Boolean
    ): Stream[F, ReadResp] = readFn(ReadReq().withOptions(ReadReq.Options()
        .withAll(AllOptions(exclusiveFrom.map(mapReadAllPosition(_)).getOrElse(startOfAll)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(ReadDirection.Forward))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(ReadReq.Empty())
    )).adaptError(extractEsException)

    def subscribeToStream[F[_]: Sync](readFn: ReadReq => Stream[F, ReadResp])(
      stream: String,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean
    ): Stream[F, ReadResp] = readFn(ReadReq().withOptions(ReadReq.Options()
      .withStream(StreamOptions(stream, exclusiveFrom.map(mapReadStreamRevision(_)).getOrElse(startOfStream)))
      .withSubscription(ReadReq.Options.SubscriptionOptions())
      .withReadDirection(mapDirection(ReadDirection.Forward))
      .withResolveLinks(resolveLinkTos)
      .withNoFilter(ReadReq.Empty())
    )).adaptError(extractEsException)

    def readAll[F[_]: Sync](readFn: ReadReq => Stream[F, ReadResp])(
      position: Position,
      direction: ReadDirection,
      maxCount: Int,
      resolveLinkTos: Boolean
    ): Stream[F, ReadResp] = readFn(ReadReq().withOptions(ReadReq.Options()
      .withAll(ReadReq.Options.AllOptions(mapReadAllPosition(position)))
      .withCount(maxCount)
      .withReadDirection(mapDirection(direction))
      .withResolveLinks(resolveLinkTos)
      .withNoFilter(ReadReq.Empty())
    )).adaptError(extractEsException)

    def readStream[F[_]: Sync](readFn: ReadReq => Stream[F, ReadResp])(
      stream: String,
      direction: ReadDirection,
      from: EventNumber,
      count: Int,
      resolveLinkTos: Boolean
    ): Stream[F, ReadResp] = readFn(ReadReq().withOptions(ReadReq.Options()
      .withStream(ReadReq.Options.StreamOptions(stream, mapReadStreamRevision(from)))
      .withCount(count)
      .withReadDirection(mapDirection(direction))
      .withResolveLinks(resolveLinkTos)
      .withNoFilter(ReadReq.Empty())
    )).adaptError(extractEsException)

    def appendToStream[F[_]: Sync](appendFn: Stream[F, AppendReq] => F[AppendResp])(
      stream: String,
      expectedRevision: StreamRevision,
      events: List[EventData]
    ): F[WriteResult] = uuidBS[F] >>= { id =>
    
      val header = AppendReq().withOptions(AppendReq.Options(id, stream, mapAppendRevision(expectedRevision)))
      val proposals = events.map(e => AppendReq().withProposedMessage(AppendReq.ProposedMessage(
        e.eventId.toBS,
        Map(Constants.Metadata.Type -> e.eventType, Constants.Metadata.IsJson -> e.data.ct.fold("false", "true")),
        e.metadata.data.toBS,
        e.data.data.toBS
      )))

      appendFn(Stream.emit(header) ++ Stream.emits(proposals)).adaptError(extractEsException) >>= mkWriteResult[F]
    }

    def softDelete[F[_]: Sync](deleteFn: DeleteReq => F[DeleteResp])(
      stream: String,
      expectedRevision: StreamRevision
    ): F[DeleteResp] = uuidBS[F] >>= { id =>
      val revision = mapDeleteRevision(expectedRevision)
      val request = DeleteReq().withOptions(DeleteReq.Options(id, stream, revision))
      deleteFn(request).adaptError(extractEsException)
    }

    def tombstone[F[_]: Sync](tombstoneFn: TombstoneReq => F[TombstoneResp])(
      stream: String,
      expectedRevision: StreamRevision
    ): F[TombstoneResp] = uuidBS[F] >>= { id =>
      val revision = mapTombstoneRevision(expectedRevision)
      val request = TombstoneReq().withOptions(TombstoneReq.Options(id, stream, revision))
      tombstoneFn(request).adaptError(extractEsException)
    }
    
    // format: on

  }

}
