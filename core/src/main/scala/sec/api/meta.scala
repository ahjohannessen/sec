package sec
package api

import java.{util => ju}
import cats.data.NonEmptyList
import cats.effect.Sync
import cats.implicits._
import io.circe._
import io.circe.parser.decode
import sec.core._
import sec.core.StreamId.Id
import sec.core.EventNumber.Exact
import sec.api.mapping._
import sec.api.Streams._

trait MetaStreams[F[_]] {

  def getMaxAge(
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[Result[MaxAge]]

  def setMaxAge(
    streamId: Id,
    age: MaxAge,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def removeMaxAge(
    streamId: Id,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def getMaxCount(
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[Result[MaxCount]]

  def setMaxCount(
    streamId: Id,
    count: MaxCount,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def removeMaxCount(
    streamId: Id,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def getCacheControl(
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[Result[CacheControl]]

  def setCacheControl(
    streamId: Id,
    cacheControl: CacheControl,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def removeCacheControl(
    streamId: Id,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def getAcl(
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[Result[StreamAcl]]

  def setAcl(
    streamId: Id,
    acl: StreamAcl,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def removeAcl(
    streamId: Id,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def getTruncateBefore(
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[Result[Exact]]

  def setTruncateBefore(
    streamId: Id,
    tb: EventNumber.Exact,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def removeTruncateBefore(
    streamId: Id,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def getCustom[T: Decoder](
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[Result[T]]

  def setCustom[T: Codec.AsObject](
    streamId: Id,
    custom: T,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def removeCustom(
    streamId: Id,
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  ///

  private[sec] def getMeta(
    id: Id,
    creds: Option[UserCredentials]
  ): F[StreamMetadataResult]

}

object MetaStreams {

  // TODO: Deal with soft deleted streams

  type Creds               = Option[UserCredentials]
  type ExpectedMetaVersion = Option[Exact]

  //======================================================================================================================

  private[sec] def apply[F[_]: Sync](s: Streams[F]): MetaStreams[F] = create[F](MetaRW[F](s))

  private[sec] def create[F[_]](meta: MetaRW[F])(implicit F: Sync[F]): MetaStreams[F] = new MetaStreams[F] {

    final val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

    //======================================================================================================================

    def getMaxAge(id: Id, creds: Option[UserCredentials]): F[Result[MaxAge]] =
      getResult(id, _.maxAge, creds)

    def setMaxAge(id: Id, ma: MaxAge, emv: ExpectedMetaVersion, creds: Option[UserCredentials]): F[Unit] =
      setMaxAge(id, ma.some, emv, creds)

    def removeMaxAge(id: Id, emv: ExpectedMetaVersion, creds: Option[UserCredentials]): F[Unit] =
      setMaxAge(id, None, emv, creds)

    def setMaxAge(id: Id, ma: Option[MaxAge], emv: ExpectedMetaVersion, uc: Option[UserCredentials]): F[Unit] =
      modify(id, _.data.withMaxAge(ma), _.state.maxAge, emv, uc) *> F.unit

    //======================================================================================================================

    def getMaxCount(id: Id, creds: Creds): F[Result[MaxCount]] =
      getResult(id, _.maxCount, creds)

    def setMaxCount(id: Id, mc: MaxCount, emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      setMaxCount(id, mc.some, emv, creds)

    def removeMaxCount(id: Id, emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      setMaxCount(id, None, emv, creds)

    def setMaxCount(id: Id, mc: Option[MaxCount], emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      modify(id, _.data.withMaxCount(mc), _.state.maxCount, emv, creds) *> F.unit

    //======================================================================================================================

    def getCacheControl(id: Id, creds: Creds): F[Result[CacheControl]] =
      getResult(id, _.cacheControl, creds)

    def setCacheControl(id: Id, cacheControl: CacheControl, emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      setCacheControl(id, cacheControl.some, emv, creds)

    def removeCacheControl(id: Id, emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      setCacheControl(id, None, emv, creds)

    def setCacheControl(id: Id, cacheControl: Option[CacheControl], emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      modify(id, _.data.withCacheControl(cacheControl), _.state.cacheControl, emv, creds) *> F.unit

    //======================================================================================================================

    def getAcl(id: Id, creds: Creds): F[Result[StreamAcl]] =
      getResult(id, _.acl, creds)

    def setAcl(id: Id, acl: StreamAcl, emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      setAcl(id, acl.some, emv, creds)

    def removeAcl(id: Id, emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      setAcl(id, None, emv, creds)

    def setAcl(id: Id, acl: Option[StreamAcl], emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      modify(id, _.data.withAcl(acl), _.state.acl, emv, creds) *> F.unit

    //======================================================================================================================

    def getTruncateBefore(id: Id, creds: Creds): F[Result[Exact]] =
      getResult(id, _.truncateBefore, creds)

    def setTruncateBefore(id: Id, truncateBefore: Exact, emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      setTruncateBefore(id, truncateBefore.some, emv, creds)

    def removeTruncateBefore(id: Id, emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      setTruncateBefore(id, None, emv, creds)

    def setTruncateBefore(id: Id, truncateBefore: Option[Exact], emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      modify(id, _.data.withTruncateBefore(truncateBefore), _.state.truncateBefore, emv, creds) *> F.unit

    //======================================================================================================================

    def getCustom[T: Decoder](id: Id, creds: Creds): F[Result[T]] =
      getMeta(id, creds) >>= (smr => smr.data.decodeCustom[F, T].map(Result(smr.version, _)))

    def setCustom[T: Codec.AsObject](id: Id, custom: T, emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      modifyF(id, _.data.modifyCustom[F, T](_ => custom.some), _.decodeCustom[F, T], emv, creds) *> F.unit

    def removeCustom(id: Id, emv: ExpectedMetaVersion, creds: Creds): F[Unit] =
      modify(id, _.data.copy(custom = None), _ => None, emv, creds) *> F.unit

    //======================================================================================================================

    //======================================================================================================================

    private[sec] def getResult[A](id: Id, fn: StreamMetadataResult => Option[A], creds: Creds): F[Result[A]] =
      getMeta(id, creds).map(smr => Result(smr.version, fn(smr)))

    private[sec] def getMeta(id: Id, creds: Creds): F[StreamMetadataResult] = {

      val decodeJson: EventRecord => F[StreamMetadata] =
        _.eventData.data.bytes.decodeUtf8.leftMap(DecodingError(_)).liftTo[F] >>= { utf8 =>
          decode[StreamMetadata](utf8).leftMap(DecodingError(_)).liftTo[F]
        }

      val recoverRead: PartialFunction[Throwable, Option[EventRecord]] = {
        case _: StreamNotFound => none[EventRecord]
      }

      meta.read(id, creds).recover(recoverRead) >>= {
        _.fold(StreamMetadataResult.empty.pure[F])(er => decodeJson(er).map(StreamMetadataResult(er.number.some, _)))
      }
    }

    private[sec] def modify[A](
      id: Id,
      mod: StreamMetadataResult => StreamMetadata,
      zoom: StreamMetadata => Option[A],
      emv: Option[Exact],
      creds: Creds
    ): F[Result[A]] =
      modifyF(id, mod(_).pure[F], zoom(_).pure[F], emv, creds)

    private[sec] def modifyF[A](
      id: Id,
      mod: StreamMetadataResult => F[StreamMetadata],
      zoom: StreamMetadata => F[Option[A]],
      emv: Option[Exact],
      creds: Creds
    ): F[Result[A]] = {

      def guardVersion(expected: Option[Exact], actual: Option[Exact]): F[Unit] =
        if (expected === actual) F.unit else F.raiseError(WrongExpectedVersion(id.metaId, expected, actual))

      def mkEventData(eventId: ju.UUID, sm: StreamMetadata): F[EventData] =
        Content
          .json(printer.print(Encoder[StreamMetadata].apply(sm)))
          .flatMap(EventData(EventType.StreamMetadata, eventId, _))
          .orFail[F](EncodingError(_))

      def write(metaVersion: Option[Exact], ed: EventData): F[WriteResult] =
        meta.write(id, metaVersion.getOrElse(StreamRevision.NoStream), ed, creds)

      def zoomA(sm: StreamMetadata, currentRevision: EventNumber.Exact): F[Result[A]] =
        zoom(sm).map(d => Result(currentRevision.some, d))

      for {
        smr         <- getMeta(id, creds)
        _           <- guardVersion(emv, smr.version)
        modified    <- mod(smr)
        eid         <- uuid[F]
        eventData   <- mkEventData(eid, modified)
        writeResult <- write(emv, eventData)
        result      <- zoomA(modified, writeResult.currentRevision)
      } yield result
    }

  }

  ///

  private[sec] trait MetaRW[F[_]] {
    def read(id: Id, creds: Creds): F[Option[EventRecord]]
    def write(id: Id, expectedRevision: StreamRevision, data: EventData, creds: Creds): F[WriteResult]
  }

  private[sec] object MetaRW {

    def apply[F[_]](s: Streams[F])(implicit F: Sync[F]): MetaRW[F] = new MetaRW[F] {

      def read(id: Id, creds: Creds): F[Option[EventRecord]] =
        s.readStream(id.metaId, EventNumber.End, Direction.Backwards, count = 1, resolveLinkTos = false, creds)
          .collect { case er: EventRecord => er }
          .compile
          .last

      def write(id: Id, er: StreamRevision, data: EventData, creds: Creds): F[WriteResult] =
        s.appendToStream(id.metaId, er, NonEmptyList.one(data), creds)
    }
  }

}

//======================================================================================================================

final case class Result[T](
  version: Option[EventNumber.Exact],
  data: Option[T]
)

//======================================================================================================================

private[sec] final case class StreamMetadataResult(
  version: Option[EventNumber.Exact],
  data: StreamMetadata
)

private[sec] object StreamMetadataResult {

  final val empty: StreamMetadataResult = StreamMetadataResult(None, StreamMetadata.empty)

  implicit final class StreamMetadataResultOps(val smr: StreamMetadataResult) extends AnyVal {
    def truncateBefore: Option[EventNumber.Exact] = smr.data.state.truncateBefore
    def maxAge: Option[MaxAge]                    = smr.data.state.maxAge
    def maxCount: Option[MaxCount]                = smr.data.state.maxCount
    def cacheControl: Option[CacheControl]        = smr.data.state.cacheControl
    def acl: Option[StreamAcl]                    = smr.data.state.acl
  }

}
