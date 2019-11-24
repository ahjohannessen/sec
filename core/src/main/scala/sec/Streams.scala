package sec

import cats.data.NonEmptyList
import cats.effect.Sync
import cats.implicits._
import io.circe._
import io.circe.parser.decode
import io.grpc.Metadata
import fs2.Stream
import com.eventstore.client.streams._
import sec.core._
import sec.syntax.StreamsSyntax
import sec.grpc._
import sec.grpc.mapping._
import sec.grpc.mapping.Streams._

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

  // Temporary

  private[sec] def getStreamMetadata(
    stream: String,
    creds: Option[UserCredentials]
  ): F[Option[Streams.StreamMetadataResult]]

  private[sec] def setStreamMetadata(
    stream: String,
    expectedRevision: StreamRevision,
    data: StreamMetaData,
    creds: Option[UserCredentials]
  ): F[Unit]

}

object Streams {

  implicit def syntaxForStreams[F[_]](s: Streams[F]): StreamsSyntax[F] = new StreamsSyntax[F](s)

  /// Result Types

  final case class WriteResult(currentRevision: StreamRevision.Exact)
  final case class DeleteResult(position: Position.Exact)

  // Questionable
  private[sec] final case class StreamMetadataResult(
    number: EventNumber.Exact,
    data: StreamMetaData
  )

  ///

  private[sec] def apply[F[_]: Sync](
    client: StreamsFs2Grpc[F, Metadata],
    options: Options
  ): Streams[F] =
    new Impl[F](client, options)

  private[sec] final class Impl[F[_]](
    val client: StreamsFs2Grpc[F, Metadata],
    val options: Options
  )(implicit F: Sync[F])
    extends Streams[F] {

    val authFallback: Metadata                    = options.creds.toMetadata
    val auth: Option[UserCredentials] => Metadata = _.fold(authFallback)(_.toMetadata)

    private val mkEvents: Stream[F, ReadResp] => Stream[F, Event] =
      _.evalMap(_.event.map(mkEvent[F]).getOrElse(none[Event].pure[F])).unNone

    def subscribeToAll(
      exclusiveFrom: Option[Position],
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, Event] = {

      val options = ReadReq
        .Options()
        .withAll(ReadReq.Options.AllOptions(exclusiveFrom.map(mapPosition).getOrElse(startOfAll)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(ReadDirection.Forward))
        .withResolveLinks(resolveLinkTos)
        .withFilterOptionsOneof(mapReadEventFilter(filter))

      val request = ReadReq().withOptions(options)

      client.read(request, auth(creds)).through(mkEvents)
    }

    def subscribeToStream(
      stream: String,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] = {

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(stream, exclusiveFrom.map(mapEventNumber).getOrElse(startOfStream)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(ReadDirection.Forward))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(ReadReq.Empty())

      val request = ReadReq().withOptions(options)

      client.read(request, auth(creds)).through(mkEvents)
    }

    def readAll(
      position: Position,
      direction: ReadDirection,
      maxCount: Int,
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, Event] = {

      val options = ReadReq
        .Options()
        .withAll(ReadReq.Options.AllOptions(mapPosition(position)))
        .withCount(maxCount)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withFilterOptionsOneof(mapReadEventFilter(filter))

      val request = ReadReq().withOptions(options)

      client.read(request, auth(creds)).through(mkEvents)
    }

    def readStream(
      stream: String,
      direction: ReadDirection,
      from: EventNumber,
      count: Int,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, Event] = {

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(stream, mapEventNumber(from)))
        .withCount(count)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(ReadReq.Empty())

      val request = ReadReq().withOptions(options)

      client.read(request, auth(creds)).through(mkEvents)
    }

    def appendToStream(
      stream: String,
      expectedRevision: StreamRevision,
      events: NonEmptyList[EventData],
      creds: Option[UserCredentials]
    ): F[WriteResult] = {

      import Constants.Metadata.{IsJson, Type}

      val header = AppendReq().withOptions(AppendReq.Options(stream, mapAppendRevision(expectedRevision)))
      val proposals = events.map { e =>
        val id         = mapUuidString(e.eventId)
        val customMeta = e.metadata.bytes.toByteString
        val data       = e.data.bytes.toByteString
        val meta       = Map(Type -> e.eventType, IsJson -> e.isJson.fold("true", "false"))
        val proposal   = AppendReq.ProposedMessage(id.some, meta, customMeta, data)
        AppendReq().withProposedMessage(proposal)
      }

      val request = Stream.emit(header) ++ Stream.emits(proposals.toList)

      client.append(request, auth(creds)) >>= mkWriteResult[F]
    }

    def softDelete(
      stream: String,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[DeleteResult] = {
      val request = DeleteReq().withOptions(DeleteReq.Options(stream, mapDeleteRevision(expectedRevision)))

      client.delete(request, auth(creds)) >>= mkDeleteResult[F]
    }

    def hardDelete(
      stream: String,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[DeleteResult] = {
      val request = TombstoneReq().withOptions(TombstoneReq.Options(stream, mapTombstoneRevision(expectedRevision)))

      client.tombstone(request, auth(creds)) >>= mkDeleteResult[F]
    }

    private[sec] def getStreamMetadata(
      stream: String,
      creds: Option[UserCredentials]
    ): F[Option[StreamMetadataResult]] = {
      import constants.SystemStreams.MetadataPrefix
      // TODO: Fix this with strong type for stream name/id
      val metadataStream = s"$MetadataPrefix$stream"

      readStream(metadataStream, ReadDirection.Backward, EventNumber.End, 1, false, creds)
        .collect { case er: EventRecord => er }
        .evalMap { er =>
          er.eventData.data.bytes.decodeUtf8.leftMap(DecodingError(_)).liftTo[F] >>= { s =>
            decode[StreamMetaData](s).leftMap(DecodingError(_)).liftTo[F].map(StreamMetadataResult(er.number, _))
          }
        }
        .compile
        .last
        .handleError {
          case StreamNotFound(`metadataStream`) => none[StreamMetadataResult]
          case _                                => None // Deal with unexpected EOS on DATA frame from server
        }
    }

    private[sec] def setStreamMetadata(
      stream: String,
      expectedRevision: StreamRevision,
      data: StreamMetaData,
      creds: Option[UserCredentials]
    ): F[Unit] = modifyStreamMetadata(stream, expectedRevision, _ => data, creds)

    private[sec] def modifyStreamMetadata(
      stream: String,
      expectedRevision: StreamRevision,
      fn: StreamMetaData => StreamMetaData,
      creds: Option[UserCredentials]
    ): F[Unit] = uuid[F] >>= { id =>
      import constants.SystemEventTypes.StreamDeleted
      import constants.SystemStreams.MetadataPrefix
      // TODO: Fix this with strong type for stream name/id
      val metadataStream = s"$MetadataPrefix$stream"

      getStreamMetadata(metadataStream, creds) >>= { res =>
        val modified  = fn(res.fold(StreamMetaData.empty)(_.data))
        val json      = Encoder[StreamMetaData].apply(modified)
        val printer   = Printer.noSpaces.copy(dropNullValues = true)
        val eventData = Content.Json(printer.print(json)) >>= (EventData(StreamDeleted, id, _))

        eventData.leftMap(EncodingError(_)).liftTo[F] >>= { d =>
          appendToStream(metadataStream, expectedRevision, NonEmptyList.one(d), creds) *> F.unit
        }
      }
    }

  }
}
