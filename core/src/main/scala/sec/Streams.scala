package sec

import cats.data.NonEmptyList
import cats.effect.ConcurrentEffect
import cats.implicits._
import io.grpc.{ManagedChannel, Metadata}
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
  ): Stream[F, ReadResp] // temp

  def subscribeToStream(
    stream: String,
    exclusiveFrom: Option[EventNumber],
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, ReadResp] // temp

  def readAll(
    position: Position,
    direction: ReadDirection,
    maxCount: Int,
    resolveLinkTos: Boolean,
    filter: Option[EventFilter],
    creds: Option[UserCredentials]
  ): Stream[F, ReadResp] // temp

  def readStream(
    stream: String,
    direction: ReadDirection,
    from: EventNumber,
    count: Int,
    resolveLinkTos: Boolean,
    creds: Option[UserCredentials]
  ): Stream[F, ReadResp] // temp

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
  ): F[DeleteResp] // temp

  def tombstone(
    stream: String,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[TombstoneResp] // temp

}

object Streams {

  implicit def syntaxForStreams[F[_]](s: Streams[F]): StreamsSyntax[F] = new StreamsSyntax[F](s)

  ///

  def apply[F[_]: ConcurrentEffect](
    channel: ManagedChannel,
    settings: Settings
  ): Streams[F] =
    Streams(StreamsFs2Grpc.client[F, Metadata](channel, identity, identity, convertToEs), settings)

  private[sec] def apply[F[_]: ErrorM](
    client: StreamsFs2Grpc[F, Metadata],
    settings: Settings
  ): Streams[F] = new Streams[F] {

    val authFallback: Metadata                    = settings.creds.toMetadata
    val auth: Option[UserCredentials] => Metadata = _.fold(authFallback)(_.toMetadata)

    def subscribeToAll(
      exclusiveFrom: Option[Position],
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, ReadResp] = {

      val options = ReadReq
        .Options()
        .withAll(ReadReq.Options.AllOptions(exclusiveFrom.map(mapPosition).getOrElse(startOfAll)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(ReadDirection.Forward))
        .withResolveLinks(resolveLinkTos)
        .withFilterOptionsOneof(mapReadEventFilter(filter))

      val request = ReadReq().withOptions(options)

      client.read(request, auth(creds))
    }

    def subscribeToStream(
      stream: String,
      exclusiveFrom: Option[EventNumber],
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, ReadResp] = {

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(stream, exclusiveFrom.map(mapEventNumber).getOrElse(startOfStream)))
        .withSubscription(ReadReq.Options.SubscriptionOptions())
        .withReadDirection(mapDirection(ReadDirection.Forward))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(ReadReq.Empty())

      val request = ReadReq().withOptions(options)

      client.read(request, auth(creds))
    }

    def readAll(
      position: Position,
      direction: ReadDirection,
      maxCount: Int,
      resolveLinkTos: Boolean,
      filter: Option[EventFilter],
      creds: Option[UserCredentials]
    ): Stream[F, ReadResp] = {

      val options = ReadReq
        .Options()
        .withAll(ReadReq.Options.AllOptions(mapPosition(position)))
        .withCount(maxCount)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withFilterOptionsOneof(mapReadEventFilter(filter))

      val request = ReadReq().withOptions(options)

      client.read(request, auth(creds))
    }

    def readStream(
      stream: String,
      direction: ReadDirection,
      from: EventNumber,
      count: Int,
      resolveLinkTos: Boolean,
      creds: Option[UserCredentials]
    ): Stream[F, ReadResp] = {

      val options = ReadReq
        .Options()
        .withStream(ReadReq.Options.StreamOptions(stream, mapEventNumber(from)))
        .withCount(count)
        .withReadDirection(mapDirection(direction))
        .withResolveLinks(resolveLinkTos)
        .withNoFilter(ReadReq.Empty())

      val request = ReadReq().withOptions(options)

      client.read(request, auth(creds))
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
        val customMeta = e.metadata.data.toByteString
        val data       = e.data.data.toByteString
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
    ): F[DeleteResp] = {
      val request = DeleteReq().withOptions(DeleteReq.Options(stream, mapDeleteRevision(expectedRevision)))

      client.delete(request, auth(creds))
    }

    def tombstone(
      stream: String,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[TombstoneResp] = {
      val request = TombstoneReq().withOptions(TombstoneReq.Options(stream, mapTombstoneRevision(expectedRevision)))

      client.tombstone(request, auth(creds))
    }
  }

}
