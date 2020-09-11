/*
 * Copyright 2020 Alex Henning Johannessen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sec
package api

import java.{util => ju}
import cats.data.NonEmptyList
import cats.effect.Sync
import cats.syntax.all._
import io.circe._
import io.circe.parser.decode
import sec.core._
import sec.core.StreamId.Id
import sec.core.EventNumber.Exact
import sec.core.StreamId.MetaId
import sec.api.exceptions.StreamNotFound
import sec.api.mapping._
import sec.api.Streams._

trait MetaStreams[F[_]] {

  def getMaxAge(
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[MetaStreams.Result[MaxAge]]

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
  ): F[MetaStreams.Result[MaxCount]]

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
  ): F[MetaStreams.Result[CacheControl]]

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
  ): F[MetaStreams.Result[StreamAcl]]

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
  ): F[MetaStreams.Result[Exact]]

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
  ): F[MetaStreams.Result[T]]

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

// TODO: Deal with soft deleted streams

object MetaStreams {

  //======================================================================================================================

  final case class Result[T](
    version: Option[EventNumber.Exact],
    data: Option[T]
  )

  //======================================================================================================================

  private[sec] type Creds               = Option[UserCredentials]
  private[sec] type ExpectedMetaVersion = Option[Exact]

  //======================================================================================================================

  private[sec] def apply[F[_]: Sync](s: Streams[F]): MetaStreams[F] = create[F](MetaRW[F](s))

  private[sec] def create[F[_]](meta: MetaRW[F])(implicit F: Sync[F]): MetaStreams[F] = new MetaStreams[F] {

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

      val recoverRead: PartialFunction[Throwable, Option[EventRecord]] = { case _: StreamNotFound =>
        none[EventRecord]
      }

      meta.read(id.metaId, creds).recover(recoverRead) >>= {
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
    ): F[Result[A]] =
      for {
        smr         <- getMeta(id, creds)
        modified    <- mod(smr)
        eid         <- uuid[F]
        eventData   <- mkEventData[F](eid, modified)
        writeResult <- meta.write(id.metaId, emv.getOrElse(StreamRevision.NoStream), eventData, creds)
        result      <- zoom(modified).map(Result(writeResult.currentRevision.some, _))
      } yield result

  }

  ///

  private[sec] def mkEventData[F[_]: ErrorA](eventId: ju.UUID, sm: StreamMetadata): F[EventData] = {
    val json = jsonPrinter.print(Encoder[StreamMetadata].apply(sm))
    Content.jsonF[F](json).map(EventData(EventType.StreamMetadata, eventId, _))
  }

  ///

  private[sec] trait MetaRW[F[_]] {
    def read(mid: MetaId, creds: Creds): F[Option[EventRecord]]
    def write(mid: MetaId, er: StreamRevision, data: EventData, creds: Creds): F[WriteResult]
  }

  private[sec] object MetaRW {

    def apply[F[_]](s: Streams[F])(implicit F: Sync[F]): MetaRW[F] = new MetaRW[F] {

      def read(mid: MetaId, creds: Creds): F[Option[EventRecord]] =
        s.readStream(mid, EventNumber.End, Direction.Backwards, maxCount = 1, resolveLinkTos = false, creds)
          .collect { case er: EventRecord => er }
          .compile
          .last

      def write(mid: MetaId, er: StreamRevision, data: EventData, creds: Creds): F[WriteResult] =
        s.appendToStream(mid, er, NonEmptyList.one(data), creds)
    }
  }

}

//======================================================================================================================

final private[sec] case class StreamMetadataResult(
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
