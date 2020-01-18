package sec
package api

import java.{util => ju}
import scala.concurrent.duration._
import cats.Endo
import cats.data.NonEmptyList
import cats.effect.Sync
import cats.implicits._
import io.circe._
import io.circe.parser.decode
import sec.core._
import sec.api.mapping._
import sec.api.Streams._
import cats.data.Kleisli

trait MetaStreams[F[_]] {

  def getMaxAge(
    streamId: StreamId.Id,
    creds: Option[UserCredentials]
  ): F[Result[MaxAge]]

  def setMaxAge(
    streamId: StreamId.Id,
    age: Option[FiniteDuration],
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def modifyMaxAge(
    streamId: StreamId.Id,
    fn: Endo[Option[FiniteDuration]],
    creds: Option[UserCredentials]
  ): F[Unit]

  //

  def getMaxCount(
    streamId: StreamId.Id,
    creds: Option[UserCredentials]
  ): F[Result[MaxCount]]

  def setMaxCount(
    streamId: StreamId.Id,
    count: Option[Int],
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def modifyMaxCount(
    streamId: StreamId.Id,
    fn: Endo[Option[Int]],
    creds: Option[UserCredentials]
  ): F[Unit]

  //

  def getCacheControl(
    streamId: StreamId.Id,
    creds: Option[UserCredentials]
  ): F[Result[CacheControl]]

  def setCacheControl(
    streamId: StreamId.Id,
    cacheControl: Option[FiniteDuration],
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def modifyCacheControl(
    streamId: StreamId.Id,
    fn: Endo[Option[FiniteDuration]],
    creds: Option[UserCredentials]
  ): F[Unit]

  //

  def getAcl(
    streamId: StreamId.Id,
    creds: Option[UserCredentials]
  ): F[Result[StreamAcl]]

  def setAcl(
    streamId: StreamId.Id,
    acl: Option[StreamAcl],
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def modifyAcl(
    streamId: StreamId.Id,
    fn: Endo[Option[StreamAcl]],
    creds: Option[UserCredentials]
  ): F[Unit]

  //

  def getTruncateBefore(
    streamId: StreamId.Id,
    creds: Option[UserCredentials]
  ): F[Result[EventNumber.Exact]]

  def setTruncateBefore(
    streamId: StreamId.Id,
    tb: Option[EventNumber.Exact],
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def modifyTruncateBefore(
    streamId: StreamId.Id,
    fn: Endo[Option[EventNumber.Exact]],
    creds: Option[UserCredentials]
  ): F[Unit]

  //

  def getCustom[T: Decoder](
    streamId: StreamId.Id,
    creds: Option[UserCredentials]
  ): F[Result[T]]

  def setCustom[T: Codec.AsObject](
    streamId: StreamId.Id,
    custom: Option[T],
    expectedMetaVersion: Option[EventNumber.Exact],
    creds: Option[UserCredentials]
  ): F[Unit]

  def modifyCustom[T: Codec.AsObject](
    streamId: StreamId.Id,
    fn: Endo[Option[T]],
    creds: Option[UserCredentials]
  ): F[Unit]

  ///

  private[sec] def getStreamMetadata(
    id: StreamId.Id,
    creds: Option[UserCredentials]
  ): F[StreamMetadataResult]

}

object MetaStreams {

  private final val printer: Printer = Printer.noSpaces.copy(dropNullValues = true)

  //======================================================================================================================

  final case class ValidationError(msg: String) extends RuntimeException(msg)

  //======================================================================================================================

  private[sec] def apply[F[_]: Sync](s: Streams[F]): MetaStreams[F] = create[F](MetaRW[F](s))

  private[sec] def create[F[_]](meta: MetaRW[F])(implicit F: Sync[F]): MetaStreams[F] = new MetaStreams[F] {

    import VersionCheck._

    implicit final class OptionOps[A](val optA: Option[A]) {
      def mkOrFail[B](fn: A => Attempt[B]): F[Option[B]] =
        optA.traverse(fn(_).orFail[F](ValidationError))
    }

    //======================================================================================================================

    def getMaxAge(
      id: StreamId.Id,
      creds: Option[UserCredentials]
    ): F[Result[MaxAge]] =
      getStreamMetadata(id, _.maxAge, creds)

    def setMaxAge(
      id: StreamId.Id,
      age: Option[FiniteDuration],
      expectedMetaVersion: Option[EventNumber.Exact],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadata(id, { smr =>
        age.mkOrFail(MaxAge(_)).map(smr.data.withMaxAge)
      }, _.state.maxAge, ExplicitCheck(expectedMetaVersion), creds) *> F.unit

    def modifyMaxAge(
      id: StreamId.Id,
      fn: Endo[Option[FiniteDuration]],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadata(id, { smr =>
        fn(smr.maxAge.map(_.value)).mkOrFail(MaxAge(_)).map(smr.data.withMaxAge)
      }, _.state.maxAge, ImplicitCheck, creds) *> F.unit

    //======================================================================================================================

    def getMaxCount(
      id: StreamId.Id,
      creds: Option[UserCredentials]
    ): F[Result[MaxCount]] =
      getStreamMetadata(id, _.maxCount, creds)

    def setMaxCount(
      id: StreamId.Id,
      count: Option[Int],
      expectedMetaVersion: Option[EventNumber.Exact],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadata(id, { smr =>
        count.mkOrFail(MaxCount(_)).map(smr.data.withMaxCount)
      }, _.state.maxAge, ExplicitCheck(expectedMetaVersion), creds) *> F.unit

    def modifyMaxCount(
      id: StreamId.Id,
      fn: Endo[Option[Int]],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadata(id, { smr =>
        fn(smr.maxCount.map(_.value)).mkOrFail(MaxCount(_)).map(smr.data.withMaxCount)
      }, _.state.maxCount, ImplicitCheck, creds) *> F.unit

    //======================================================================================================================

    def getCacheControl(
      id: StreamId.Id,
      creds: Option[UserCredentials]
    ): F[Result[CacheControl]] =
      getStreamMetadata(id, _.cacheControl, creds)

    def setCacheControl(
      id: StreamId.Id,
      cacheControl: Option[FiniteDuration],
      expectedMetaVersion: Option[EventNumber.Exact],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadata(id, { smr =>
        cacheControl.mkOrFail(CacheControl(_)).map(smr.data.withCacheControl)
      }, _.state.cacheControl, ExplicitCheck(expectedMetaVersion), creds) *> F.unit

    def modifyCacheControl(
      id: StreamId.Id,
      fn: Endo[Option[FiniteDuration]],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadata(id, { smr =>
        fn(smr.cacheControl.map(_.value)).mkOrFail(CacheControl(_)).map(smr.data.withCacheControl)
      }, _.state.cacheControl, ImplicitCheck, creds) *> F.unit

    //======================================================================================================================

    def getAcl(
      id: StreamId.Id,
      creds: Option[UserCredentials]
    ): F[Result[StreamAcl]] =
      getStreamMetadata(id, _.acl, creds)

    def setAcl(
      id: StreamId.Id,
      acl: Option[StreamAcl],
      expectedMetaVersion: Option[EventNumber.Exact],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadata(id, { _.data.withAcl(acl).pure[F] }, _.state.acl, ExplicitCheck(expectedMetaVersion), creds) *> F.unit

    def modifyAcl(
      id: StreamId.Id,
      fn: Endo[Option[StreamAcl]],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadata(id, { smr =>
        smr.data.withAcl(fn(smr.acl)).pure[F]
      }, _.state.acl, ImplicitCheck, creds) *> F.unit

    //======================================================================================================================

    def getTruncateBefore(
      id: StreamId.Id,
      creds: Option[UserCredentials]
    ): F[Result[EventNumber.Exact]] =
      getStreamMetadata(id, _.truncateBefore, creds)

    def setTruncateBefore(
      id: StreamId.Id,
      truncateBefore: Option[EventNumber.Exact],
      expectedMetaVersion: Option[EventNumber.Exact],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadata(
        id,
        _.data.withTruncateBefore(truncateBefore).pure[F],
        _.state.truncateBefore,
        ExplicitCheck(expectedMetaVersion),
        creds
      ) *> F.unit

    def modifyTruncateBefore(
      id: StreamId.Id,
      fn: Endo[Option[EventNumber.Exact]],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadata(id, { smr =>
        smr.data.withTruncateBefore(fn(smr.truncateBefore)).pure[F]
      }, _.state.truncateBefore, ImplicitCheck, creds) *> F.unit

    //======================================================================================================================

    def getCustom[T: Decoder](
      id: StreamId.Id,
      creds: Option[UserCredentials]
    ): F[Result[T]] =
      getStreamMetadata(id, creds) >>= (smr => smr.data.decodeCustom[F, T].map(Result(smr.version, _)))

    def setCustom[T: Codec.AsObject](
      id: StreamId.Id,
      custom: Option[T],
      expectedMetaVersion: Option[EventNumber.Exact],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadataK(
        id,
        Kleisli(_.data.modifyCustom[F, T](_ => custom)),
        Kleisli(_.decodeCustom[F, T]),
        ExplicitCheck(expectedMetaVersion),
        creds
      ) *> F.unit

    def modifyCustom[T: Codec.AsObject](
      id: StreamId.Id,
      fn: Endo[Option[T]],
      creds: Option[UserCredentials]
    ): F[Unit] =
      modifyStreamMetadataK(
        id,
        Kleisli(_.data.modifyCustom[F, T](fn)),
        Kleisli(_.decodeCustom[F, T]),
        ImplicitCheck,
        creds
      ) *> F.unit

    //======================================================================================================================

    private[sec] def getStreamMetadata[A](
      id: StreamId.Id,
      fn: StreamMetadataResult => Option[A],
      creds: Option[UserCredentials]
    ): F[Result[A]] =
      getStreamMetadata(id, creds).map(smr => Result(smr.version, fn(smr)))

    private[sec] def getStreamMetadata(
      id: StreamId.Id,
      creds: Option[UserCredentials]
    ): F[StreamMetadataResult] = {

      val decodeJson: EventRecord => F[StreamMetadata] =
        _.eventData.data.bytes.decodeUtf8.leftMap(DecodingError(_)).liftTo[F] >>= { utf8 =>
          decode[StreamMetadata](utf8).leftMap(DecodingError(_)).liftTo[F]
        }

      // TODO: Deal with soft deleted streams, StreamDeleted.
      val recoverRead: PartialFunction[Throwable, Option[EventRecord]] = {
        case _: StreamNotFound => none[EventRecord]
      }

      meta.read(id, creds).recover(recoverRead) >>= {
        _.fold(StreamMetadataResult.empty.pure[F])(er => decodeJson(er).map(StreamMetadataResult(er.number, _)))
      }
    }

    private[sec] def modifyStreamMetadata[A](
      id: StreamId.Id,
      mod: StreamMetadataResult => F[StreamMetadata],
      zoom: StreamMetadata => Option[A],
      versionGuard: VersionCheck,
      creds: Option[UserCredentials]
    ): F[Result[A]] =
      modifyStreamMetadataK(id, Kleisli(r => mod(r)), Kleisli(r => zoom(r).pure[F]), versionGuard, creds)

    private[sec] def modifyStreamMetadataK[A](
      id: StreamId.Id,
      mod: Kleisli[F, StreamMetadataResult, StreamMetadata],
      zoom: Kleisli[F, StreamMetadata, Option[A]],
      versionGuard: VersionCheck,
      creds: Option[UserCredentials]
    ): F[Result[A]] = {

      def mkEventData(eventId: ju.UUID, sm: StreamMetadata): F[NonEmptyList[EventData]] =
        Content
          .json(printer.print(Encoder[StreamMetadata].apply(sm)))
          .flatMap(EventData(EventType.StreamMetadata, eventId, _).map(NonEmptyList.one))
          .orFail[F](EncodingError(_))

      def write(metaVersion: Option[EventNumber.Exact], nel: NonEmptyList[EventData]): F[WriteResult] =
        meta.write(id, metaVersion.getOrElse(StreamRevision.NoStream), nel, creds)

      def zoomA(sm: StreamMetadata, currentRevision: EventNumber.Exact): F[Result[A]] =
        zoom(sm).map(d => Result(currentRevision.some, d))

      val action = for {
        smr         <- getStreamMetadata(id, creds)
        _           <- smr.guardVersion[F](id, versionGuard)
        modified    <- mod(smr)
        eid         <- uuid[F]
        eventData   <- mkEventData(eid, modified)
        writeResult <- write(smr.version, eventData)
        result      <- zoomA(modified, writeResult.currentRevision)
      } yield result

      def runWithRecovery(attempts: Int): F[Result[A]] = action.recoverWith {
        case _: WrongExpectedVersion if versionGuard.isImplicit && attempts < 10 => runWithRecovery(attempts + 1)
      }

      runWithRecovery(0)
    }

  }

  ///

  private[sec] trait MetaRW[F[_]] {

    def read(
      id: StreamId.Id,
      creds: Option[UserCredentials]
    ): F[Option[EventRecord]]

    def write(
      id: StreamId.Id,
      expectedRevision: StreamRevision,
      data: NonEmptyList[EventData],
      creds: Option[UserCredentials]
    ): F[WriteResult]

  }

  private[sec] object MetaRW {

    def apply[F[_]](s: Streams[F])(implicit F: Sync[F]): MetaRW[F] = new MetaRW[F] {

      def read(
        id: StreamId.Id,
        creds: Option[UserCredentials]
      ): F[Option[EventRecord]] =
        s.readStream(id.meta, EventNumber.End, Direction.Backwards, count = 1, resolveLinkTos = false, creds)
          .collect { case er: EventRecord => er }
          .compile
          .last

      def write(
        id: StreamId.Id,
        er: StreamRevision,
        data: NonEmptyList[EventData],
        creds: Option[UserCredentials]
      ): F[WriteResult] = s.appendToStream(id.meta, er, data, creds)

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

  def apply(version: EventNumber.Exact, data: StreamMetadata): StreamMetadataResult =
    StreamMetadataResult(version.some, data)

  final val empty: StreamMetadataResult = StreamMetadataResult(None, StreamMetadata.empty)

  implicit final class StreamMetadataResultOps(val smr: StreamMetadataResult) extends AnyVal {

    def truncateBefore: Option[EventNumber.Exact] = smr.data.state.truncateBefore
    def maxAge: Option[MaxAge]                    = smr.data.state.maxAge
    def maxCount: Option[MaxCount]                = smr.data.state.maxCount
    def cacheControl: Option[CacheControl]        = smr.data.state.cacheControl
    def acl: Option[StreamAcl]                    = smr.data.state.acl

    def guardVersion[F[_]](id: StreamId.Id, vc: VersionCheck)(implicit F: ErrorA[F]): F[Unit] = {

      def verify(expected: Option[EventNumber.Exact]): F[Unit] =
        if (expected === smr.version) F.unit
        else F.raiseError(WrongExpectedVersion(id.meta, expected, smr.version))

      vc.fold(verify, F.unit)
    }

  }
}

//======================================================================================================================

private[sec] sealed trait VersionCheck
private[sec] object VersionCheck {

  final case class ExplicitCheck(expectedMetaVersion: Option[EventNumber.Exact]) extends VersionCheck
  case object ImplicitCheck                                                      extends VersionCheck

  implicit final class VersionGuardOps(val vg: VersionCheck) extends AnyVal {

    def isImplicit: Boolean = fold(_ => false, true)

    def fold[A](explicitCheck: Option[EventNumber.Exact] => A, implicitCheck: => A): A = vg match {
      case ExplicitCheck(emv) => explicitCheck(emv)
      case ImplicitCheck      => implicitCheck
    }
  }

}

//======================================================================================================================
