package sec

import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import fs2.Stream
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
  ): F[com.eventstore.client.streams.AppendResp] // temp

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

  import io.grpc.Metadata
  import com.eventstore.client.streams._

  implicit def syntaxForESClient[F[_]](esc: EsClient[F]): sec.syntax.EsClientSyntax[F] =
    new sec.syntax.EsClientSyntax[F](esc)

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
      impl.subscribeToAll[F](client.read(_, auth(creds)))(exclusiveFrom, resolveLinkTos)

    def subscribeToStream(
      stream: String,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, ReadResp] =
      impl.subscribeToStream(client.read(_, auth(creds)))(stream, exclusiveFrom, resolveLinkTos)

    def readAll(
      position: Position,
      direction: ReadDirection,
      maxCount: Int,
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, ReadResp] =
      impl.readAll[F](client.read(_, auth(creds)))(position, direction, maxCount, resolveLinkTos)

    def readStream(
      stream: String,
      direction: ReadDirection,
      from: EventNumber,
      count: Int,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, ReadResp] =
      impl.readStream[F](client.read(_, auth(creds)))(stream, direction, from, count, resolveLinkTos)

    def appendToStream(
      stream: String,
      expectedRevision: StreamRevision,
      events: NonEmptyList[EventData],
      creds: Option[UserCredentials]
    ): F[AppendResp] =
      impl.appendToStream[F](client.append(_, auth(creds)))(stream, expectedRevision, events.toList)

    def softDelete(
      stream: String,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[DeleteResp] =
      impl.softDelete[F](client.delete(_, auth(creds)))(stream, expectedRevision)

    def tombstone(
      stream: String,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[TombstoneResp] =
      impl.tombstone[F](client.tombstone(_, auth(creds)))(stream, expectedRevision)
  }

  // format: off

  private[sec] object impl {

    import com.google.protobuf.ByteString
    import ReadReq.Options.AllOptions
    import ReadReq.Options.StreamOptions

    private val startOfAll    = AllOptions.AllOptions.Start(ReadReq.Empty())
    private val endOfAll      = AllOptions.AllOptions.Position(ReadReq.Options.Position(-1L, -1L))
    private val startOfStream = StreamOptions.RevisionOptions.Start(ReadReq.Empty())
    private val endOfStream   = StreamOptions.RevisionOptions.Revision(-1L)

    ///
    
    def subscribeToAll[F[_]](readFn: ReadReq => Stream[F, ReadResp])(
      exclusiveFrom: Option[Position],
      resolveLinkTos: Boolean
    ): Stream[F, ReadResp] = readFn(ReadReq().withOptions(ReadReq.Options()
        .withAll(AllOptions(exclusiveFrom.map(mapReadAllPosition(_)).getOrElse(startOfAll)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(ReadDirection.Forward))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(ReadReq.Empty())
    ))

    def subscribeToStream[F[_]](readFn: ReadReq => Stream[F, ReadResp])(
      stream: String,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean
    ): Stream[F, ReadResp] = readFn(ReadReq().withOptions(ReadReq.Options()
      .withStream(StreamOptions(stream, exclusiveFrom.map(mapReadStreamRevision(_)).getOrElse(startOfStream)))
      .withSubscription(ReadReq.Options.SubscriptionOptions())
      .withReadDirection(mapDirection(ReadDirection.Forward))
      .withResolveLinks(resolveLinkTos)
      .withNoFilter(ReadReq.Empty())
    ))

    def readAll[F[_]](readFn: ReadReq => Stream[F, ReadResp])(
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
    ))

    def readStream[F[_]](readFn: ReadReq => Stream[F, ReadResp])(
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
    ))

    def appendToStream[F[_]: Sync](appendFn: Stream[F, AppendReq] => F[AppendResp])(
      stream: String,
      expectedRevision: StreamRevision,
      events: List[EventData]
    ): F[AppendResp] = uuidBS[F] >>= { id =>
      val header = AppendReq().withOptions(AppendReq.Options(id, stream, mapAppendRevision(expectedRevision)))
      val proposals = events.map(e => AppendReq().withProposedMessage(AppendReq.ProposedMessage(
        e.eventId.toBS,
        Map(Constants.Metadata.Type -> e.eventType, Constants.Metadata.IsJson -> e.data.ct.fold("false", "true")),
        e.metadata.data.toBS,
        e.data.data.toBS
      )))

      appendFn(Stream.emit(header) ++ Stream.emits(proposals))
    }

    def softDelete[F[_]: Sync](deleteFn: DeleteReq => F[DeleteResp])(
      stream: String,
      expectedRevision: StreamRevision
    ): F[DeleteResp] = uuidBS[F] >>= { id =>
      deleteFn(DeleteReq().withOptions(DeleteReq.Options(id, stream, mapDeleteRevision(expectedRevision))))
    }

    def tombstone[F[_]: Sync](tombstoneFn: TombstoneReq => F[TombstoneResp])(
      stream: String,
      expectedRevision: StreamRevision
    ): F[TombstoneResp] = uuidBS[F] >>= { id =>
      tombstoneFn(TombstoneReq().withOptions(TombstoneReq.Options(id, stream, mapTombstoneRevision(expectedRevision))))
    }
    
    // format: on
    ///

    val mapReadAllPosition: Position => ReadReq.Options.AllOptions.AllOptions = {
      case Position.Start       => startOfAll
      case Position.Exact(c, p) => ReadReq.Options.AllOptions.AllOptions.Position(ReadReq.Options.Position(c, p))
      case Position.End         => endOfAll
    }

    val mapReadStreamRevision: EventNumber => ReadReq.Options.StreamOptions.RevisionOptions = {
      case EventNumber.Start      => startOfStream
      case EventNumber.Exact(rev) => StreamOptions.RevisionOptions.Revision(rev)
      case EventNumber.End        => endOfStream
    }

    val mapAppendRevision: StreamRevision => AppendReq.Options.ExpectedStreamRevision = {
      case StreamRevision.Version(v)   => AppendReq.Options.ExpectedStreamRevision.Revision(v)
      case StreamRevision.NoStream     => AppendReq.Options.ExpectedStreamRevision.NoStream(AppendReq.Empty())
      case StreamRevision.StreamExists => AppendReq.Options.ExpectedStreamRevision.StreamExists(AppendReq.Empty())
      case StreamRevision.Any          => AppendReq.Options.ExpectedStreamRevision.Any(AppendReq.Empty())
    }

    val mapDeleteRevision: StreamRevision => DeleteReq.Options.ExpectedStreamRevision = {
      case StreamRevision.Version(v)   => DeleteReq.Options.ExpectedStreamRevision.Revision(v)
      case StreamRevision.NoStream     => DeleteReq.Options.ExpectedStreamRevision.NoStream(DeleteReq.Empty())
      case StreamRevision.StreamExists => DeleteReq.Options.ExpectedStreamRevision.StreamExists(DeleteReq.Empty())
      case StreamRevision.Any          => DeleteReq.Options.ExpectedStreamRevision.Any(DeleteReq.Empty())
    }

    val mapTombstoneRevision: StreamRevision => TombstoneReq.Options.ExpectedStreamRevision = {
      case StreamRevision.Version(v)   => TombstoneReq.Options.ExpectedStreamRevision.Revision(v)
      case StreamRevision.NoStream     => TombstoneReq.Options.ExpectedStreamRevision.NoStream(TombstoneReq.Empty())
      case StreamRevision.StreamExists => TombstoneReq.Options.ExpectedStreamRevision.StreamExists(TombstoneReq.Empty())
      case StreamRevision.Any          => TombstoneReq.Options.ExpectedStreamRevision.Any(TombstoneReq.Empty())
    }

    val mapDirection: ReadDirection => ReadReq.Options.ReadDirection = {
      case ReadDirection.Forward  => ReadReq.Options.ReadDirection.Forwards
      case ReadDirection.Backward => ReadReq.Options.ReadDirection.Backwards
    }

    def mapPosition(exact: Position.Exact): ReadReq.Options.AllOptions.AllOptions.Position =
      ReadReq.Options.AllOptions.AllOptions.Position(ReadReq.Options.Position(exact.commit, exact.prepare))

    def mapRevision(exact: EventNumber.Exact): ReadReq.Options.StreamOptions.RevisionOptions.Revision =
      ReadReq.Options.StreamOptions.RevisionOptions.Revision(exact.revision)

    def uuidBS[F[_]: Sync]: F[ByteString] = uuid[F].map(_.toBS)

  }

}
