package sec

import cats.Show
import cats.data.NonEmptyList
import cats.implicits._
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

}

object Streams {

  implicit def syntaxForStreams[F[_]](s: Streams[F]): StreamsSyntax[F] = new StreamsSyntax[F](s)

  /// Result Types

  final case class WriteResult(
    currentRevision: StreamRevision.Exact
  )

  object WriteResult {
    implicit val showForWriteResult: Show[WriteResult] = Show.show { wr =>
      s"WriteResult(currentRevision = ${wr.currentRevision.value})"
    }
  }

  final case class DeleteResult(position: Position.Exact)

  object DeleteResult {
    implicit val showForDeleteResult: Show[DeleteResult] = Show.show { dr =>
      s"DeleteResult(commit = ${dr.position.commit}, prepare = ${dr.position.prepare})"
    }
  }

  ///

  private[sec] def apply[F[_]: ErrorM](
    client: StreamsFs2Grpc[F, Metadata],
    options: Options
  ): Streams[F] =
    new Impl[F](client, options)

  private[sec] final class Impl[F[_]: ErrorM](
    val client: StreamsFs2Grpc[F, Metadata],
    val options: Options
  ) extends Streams[F] {

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
  }

}
