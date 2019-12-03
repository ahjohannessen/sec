package sec
package api

import cats.data.NonEmptyList
import cats.effect.Sync
import cats.implicits._
import io.circe.{Decoder, Encoder, Printer}
import io.circe.parser.decode
import sec.core._
import sec.api.mapping._
import sec.api.Streams._

/* TODO:
  - Questionable API, too low-level, perhaps.
  - Lenses should be added for StreamState, if it should be surfaced in that shape.
  - Name StreamState is perhaps not appropriate or too strange.
  - Use strong type for stream name/id
  - Surface custom metadata
  - Verify that we get metadata stream back - questionable
 */

private[sec] trait StreamMeta[F[_]] {

  def getStreamMetadata(
    stream: String,
    creds: Option[UserCredentials]
  ): F[Option[StreamMeta.Result]]

  def setStreamMetadata(
    stream: String,
    data: StreamState,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[Unit]

  def modifyStreamMetadata(
    stream: String,
    modFn: StreamState => StreamState,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[Unit]

}

private[sec] object StreamMeta {

  // Questionable
  final case class Result(
    number: EventNumber.Exact,
    data: StreamState
  )

  ///

  private[sec] trait MetaRW[F[_]] {

    def readMeta(
      stream: String,
      creds: Option[UserCredentials]
    ): F[Option[EventRecord]]

    def writeMeta(
      stream: String,
      expectedRevision: StreamRevision,
      data: NonEmptyList[EventData],
      creds: Option[UserCredentials]
    ): F[WriteResult]
  }

  private[sec] object MetaRW {

    def apply[F[_]: Sync](s: Streams[F]): MetaRW[F] = new MetaRW[F] {

      def readMeta(
        stream: String,
        creds: Option[UserCredentials]
      ): F[Option[EventRecord]] =
        s.readStream(stream, ReadDirection.Backward, EventNumber.End, 1, resolveLinkTos = false, creds)
          .collect { case er: EventRecord => er }
          .compile
          .last

      def writeMeta(
        stream: String,
        expectedRevision: StreamRevision,
        data: NonEmptyList[EventData],
        creds: Option[UserCredentials]
      ): F[WriteResult] = s.appendToStream(stream, expectedRevision, data, creds)
    }
  }

  ///

  def apply[F[_]: Sync](s: Streams[F]): StreamMeta[F] = create[F](MetaRW[F](s))
  def create[F[_]](metaRW: MetaRW[F])(implicit F: Sync[F]): StreamMeta[F] = new StreamMeta[F] {

    import core.constants.SystemStreams.MetadataPrefix
    import core.constants.SystemEventTypes.{StreamMetadata => StreamMetadataType}

    val metaForStream: String => String = stream => s"$MetadataPrefix$stream"
    val recoverRead: PartialFunction[Throwable, Option[EventRecord]] = {
      case _: StreamNotFound | _: StreamDeleted => none[EventRecord]
    }

    def decodeJson[A: Decoder](er: EventRecord): F[A] =
      er.eventData.data.bytes.decodeUtf8.leftMap(DecodingError(_)).liftTo[F] >>= { utf8 =>
        decode[A](utf8).leftMap(DecodingError(_)).liftTo[F]
      }

    def getStreamMetadata(
      stream: String,
      creds: Option[UserCredentials]
    ): F[Option[Result]] =
      metaRW.readMeta(metaForStream(stream), creds).recover(recoverRead) >>= {
        _.traverse(er => decodeJson[StreamState](er).map(Result(er.number, _)))
      }

    def setStreamMetadata(
      stream: String,
      data: StreamState,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[Unit] = modifyStreamMetadata(stream, _ => data, expectedRevision, creds)

    def modifyStreamMetadata(
      stream: String,
      modFn: StreamState => StreamState,
      expectedRevision: StreamRevision,
      creds: Option[UserCredentials]
    ): F[Unit] = uuid[F] >>= { id =>
      getStreamMetadata(stream, creds) >>= { res =>
        val modified  = modFn(res.fold(StreamState.empty)(_.data))
        val json      = Encoder[StreamState].apply(modified)
        val printer   = Printer.noSpaces.copy(dropNullValues = true)
        val eventData = Content.json(printer.print(json)) >>= (EventData(StreamMetadataType, id, _))

        eventData.leftMap(EncodingError(_)).liftTo[F] >>= { d =>
          metaRW.writeMeta(metaForStream(stream), expectedRevision, NonEmptyList.one(d), creds) *> F.unit
        }
      }
    }
  }
}
