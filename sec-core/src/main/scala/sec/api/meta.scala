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
import cats.Endo
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
import MetaStreams._

trait MetaStreams[F[_]] {

  def getMaxAge(
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[ReadResult[MaxAge]]

  def setMaxAge(
    streamId: Id,
    age: MaxAge,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def removeMaxAge(
    streamId: Id,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def getMaxCount(
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[ReadResult[MaxCount]]

  def setMaxCount(
    streamId: Id,
    count: MaxCount,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def removeMaxCount(
    streamId: Id,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def getCacheControl(
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[ReadResult[CacheControl]]

  def setCacheControl(
    streamId: Id,
    cacheControl: CacheControl,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def removeCacheControl(
    streamId: Id,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[MetaStreams.WriteResult]

  def getAcl(
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[ReadResult[StreamAcl]]

  def setAcl(
    streamId: Id,
    acl: StreamAcl,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def removeAcl(
    streamId: Id,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def getTruncateBefore(
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[ReadResult[Exact]]

  def setTruncateBefore(
    streamId: Id,
    tb: Exact,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def removeTruncateBefore(
    streamId: Id,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def getCustom[T: Decoder](
    streamId: Id,
    creds: Option[UserCredentials]
  ): F[ReadResult[T]]

  def setCustom[T: Codec.AsObject](
    streamId: Id,
    custom: T,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def removeCustom(
    streamId: Id,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  ///

  private[sec] def getMeta(
    id: Id,
    creds: Option[UserCredentials]
  ): F[MetaResult]

  private[sec] def setMeta(
    id: Id,
    data: StreamMetadata,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  private[sec] def removeMeta(
    id: Id,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

}

// TODO: Deal with soft deleted streams

object MetaStreams {

  //====================================================================================================================

  type ReadResult[T]           = Option[Result[Option[T]]]
  private[sec] type MetaResult = Option[Result[StreamMetadata]]

  final case class Result[T](
    metaRevision: EventNumber.Exact,
    data: T
  )

  object Result {
    implicit final class ResultOps[A](val r: Result[A]) extends AnyVal {
      def withData[B](data: B): Result[B] = zoom(_ => data)
      def zoom[B](fn: A => B): Result[B]  = r.copy(data = fn(r.data))
    }
  }

  final case class WriteResult(
    currentMetaRevision: EventNumber.Exact
  )

  //====================================================================================================================

  private[sec] def apply[F[_]: Sync](s: Streams[F]): MetaStreams[F] = create[F](MetaRW[F](s))
  private[sec] def create[F[_]](meta: MetaRW[F])(implicit F: Sync[F]): MetaStreams[F] = new MetaStreams[F] {

    //==================================================================================================================

    def getMaxAge(id: Id, creds: Option[UserCredentials]): F[ReadResult[MaxAge]] =
      getResult(id, _.maxAge, creds)

    def setMaxAge(id: Id, ma: MaxAge, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setMaxAge(id, ma.some, er, creds)

    def removeMaxAge(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setMaxAge(id, None, er, creds)

    def setMaxAge(id: Id, ma: Option[MaxAge], er: StreamRevision, uc: Option[UserCredentials]): F[WriteResult] =
      modify(id, _.withMaxAge(ma), er, uc)

    //==================================================================================================================

    def getMaxCount(id: Id, creds: Option[UserCredentials]): F[ReadResult[MaxCount]] =
      getResult(id, _.maxCount, creds)

    def setMaxCount(id: Id, mc: MaxCount, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setMaxCount(id, mc.some, er, creds)

    def removeMaxCount(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setMaxCount(id, None, er, creds)

    def setMaxCount(id: Id, mc: Option[MaxCount], er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      modify(id, _.withMaxCount(mc), er, creds)

    //==================================================================================================================

    def getCacheControl(id: Id, creds: Option[UserCredentials]): F[ReadResult[CacheControl]] =
      getResult(id, _.cacheControl, creds)

    def setCacheControl(
      id: Id,
      cacheControl: CacheControl,
      er: StreamRevision,
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      setCacheControl(id, cacheControl.some, er, creds)

    def removeCacheControl(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setCacheControl(id, None, er, creds)

    def setCacheControl(
      id: Id,
      cacheControl: Option[CacheControl],
      er: StreamRevision,
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      modify(id, _.withCacheControl(cacheControl), er, creds)

    //==================================================================================================================

    def getAcl(id: Id, creds: Option[UserCredentials]): F[ReadResult[StreamAcl]] =
      getResult(id, _.acl, creds)

    def setAcl(id: Id, acl: StreamAcl, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setAcl(id, acl.some, er, creds)

    def removeAcl(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setAcl(id, None, er, creds)

    def setAcl(id: Id, acl: Option[StreamAcl], er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      modify(id, _.withAcl(acl), er, creds)

    //==================================================================================================================

    def getTruncateBefore(id: Id, creds: Option[UserCredentials]): F[ReadResult[Exact]] =
      getResult(id, _.truncateBefore, creds)

    def setTruncateBefore(
      id: Id,
      truncateBefore: Exact,
      er: StreamRevision,
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      setTruncateBefore(id, truncateBefore.some, er, creds)

    def removeTruncateBefore(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setTruncateBefore(id, None, er, creds)

    def setTruncateBefore(
      id: Id,
      truncateBefore: Option[Exact],
      er: StreamRevision,
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      modify(id, _.withTruncateBefore(truncateBefore), er, creds)

    //==================================================================================================================

    def getCustom[T: Decoder](id: Id, creds: Option[UserCredentials]): F[ReadResult[T]] =
      getMeta(id, creds) >>= { _.traverse(r => r.data.decodeCustom[F, T].map(r.withData)) }

    def setCustom[T: Codec.AsObject](
      id: Id,
      custom: T,
      esr: StreamRevision,
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      modifyF(id, _.modifyCustom[F, T](_ => custom.some), esr, creds)

    def removeCustom(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      modify(id, _.copy(custom = None), er, creds)

    //==================================================================================================================

    private[sec] def getResult[A](
      id: Id,
      fn: StreamMetadata => Option[A],
      creds: Option[UserCredentials]
    ): F[ReadResult[A]] =
      getMeta(id, creds).nested.map(_.zoom(fn)).value

    private[sec] def getMeta(id: Id, creds: Option[UserCredentials]): F[MetaResult] = {

      val decodeJson: EventRecord => F[StreamMetadata] =
        _.eventData.data.bytes.decodeUtf8.leftMap(DecodingError(_)).liftTo[F] >>= { utf8 =>
          decode[StreamMetadata](utf8).leftMap(DecodingError(_)).liftTo[F]
        }

      val recoverRead: PartialFunction[Throwable, Option[EventRecord]] = { case _: StreamNotFound =>
        none[EventRecord]
      }

      meta.read(id.metaId, creds).recover(recoverRead) >>= {
        _.traverse(er => decodeJson(er).map(Result(er.number, _)))
      }

    }

    private[sec] def setMeta(id: Id,
                             sm: StreamMetadata,
                             er: StreamRevision,
                             creds: Option[UserCredentials]): F[WriteResult] =
      modify(id, _ => sm, er, creds: Option[UserCredentials])

    private[sec] def removeMeta(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      modify(id, _ => StreamMetadata.empty, er, creds)

    ///

    private[sec] def modify[A](id: Id,
                               mod: Endo[StreamMetadata],
                               er: StreamRevision,
                               creds: Option[UserCredentials]): F[WriteResult] =
      modifyF(id, mod(_).pure[F], er, creds)

    private[sec] def modifyF[A](
      id: Id,
      mod: EndoF[F, StreamMetadata],
      er: StreamRevision,
      creds: Option[UserCredentials]
    ): F[WriteResult] = {
      for {
        smr         <- getMeta(id, creds)
        modified    <- mod(smr.fold(StreamMetadata.empty)(_.data))
        eid         <- uuid[F]
        eventData   <- mkEventData[F](eid, modified)
        writeResult <- meta.write(id.metaId, er, eventData, creds)
      } yield WriteResult(writeResult.currentRevision)
    }

  }

  private[sec] def mkEventData[F[_]: ErrorA](eventId: ju.UUID, sm: StreamMetadata): F[EventData] = {
    val json = jsonPrinter.print(Encoder[StreamMetadata].apply(sm))
    Content.jsonF[F](json).map(EventData(EventType.StreamMetadata, eventId, _))
  }

  ///

  private[sec] trait MetaRW[F[_]] {
    def read(mid: MetaId, creds: Option[UserCredentials]): F[Option[EventRecord]]
    def write(mid: MetaId, er: StreamRevision, data: EventData, creds: Option[UserCredentials]): F[Streams.WriteResult]
  }

  private[sec] object MetaRW {

    def apply[F[_]](s: Streams[F])(implicit F: Sync[F]): MetaRW[F] = new MetaRW[F] {

      def read(mid: MetaId, creds: Option[UserCredentials]): F[Option[EventRecord]] =
        s.readStreamBackwards(mid, EventNumber.End, maxCount = 1, resolveLinkTos = false, creds)
          .collect { case er: EventRecord => er }
          .compile
          .last

      def write(
        mid: MetaId,
        er: StreamRevision,
        data: EventData,
        creds: Option[UserCredentials]
      ): F[Streams.WriteResult] =
        s.appendToStream(mid, er, NonEmptyList.one(data), creds)
    }
  }

}
