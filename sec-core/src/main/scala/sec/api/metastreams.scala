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
  ): F[Option[ReadResult[MaxAge]]]

  def setMaxAge(
    streamId: Id,
    expectedRevision: StreamRevision,
    age: MaxAge,
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
  ): F[Option[ReadResult[MaxCount]]]

  def setMaxCount(
    streamId: Id,
    expectedRevision: StreamRevision,
    count: MaxCount,
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
  ): F[Option[ReadResult[CacheControl]]]

  def setCacheControl(
    streamId: Id,
    expectedRevision: StreamRevision,
    cacheControl: CacheControl,
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
  ): F[Option[ReadResult[StreamAcl]]]

  def setAcl(
    streamId: Id,
    expectedRevision: StreamRevision,
    acl: StreamAcl,
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
  ): F[Option[ReadResult[Exact]]]

  def setTruncateBefore(
    streamId: Id,
    expectedRevision: StreamRevision,
    truncateBefore: Exact,
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
  ): F[Option[ReadResult[T]]]

  def setCustom[T: Codec.AsObject](
    streamId: Id,
    expectedRevision: StreamRevision,
    custom: T,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def removeCustom(
    streamId: Id,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  ///

  private[sec] def getMetadata(
    id: Id,
    creds: Option[UserCredentials]
  ): F[Option[MetaResult]]

  private[sec] def setMetadata(
    id: Id,
    expectedRevision: StreamRevision,
    data: StreamMetadata,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  private[sec] def removeMetadata(
    id: Id,
    expectedRevision: StreamRevision,
    creds: Option[UserCredentials]
  ): F[WriteResult]

}

object MetaStreams {

  //====================================================================================================================

  type ReadResult[T]           = Result[Option[T]]
  private[sec] type MetaResult = Result[StreamMetadata]

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

    def getMaxAge(id: Id, creds: Option[UserCredentials]): F[Option[ReadResult[MaxAge]]] =
      getResult(id, _.maxAge, creds)

    def setMaxAge(id: Id, er: StreamRevision, ma: MaxAge, creds: Option[UserCredentials]): F[WriteResult] =
      setMaxAge(id, er, ma.some, creds)

    def removeMaxAge(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setMaxAge(id, er, None, creds)

    def setMaxAge(id: Id, er: StreamRevision, ma: Option[MaxAge], uc: Option[UserCredentials]): F[WriteResult] =
      modify(id, er, _.setMaxAge(ma), uc)

    //==================================================================================================================

    def getMaxCount(id: Id, creds: Option[UserCredentials]): F[Option[ReadResult[MaxCount]]] =
      getResult(id, _.maxCount, creds)

    def setMaxCount(id: Id, er: StreamRevision, mc: MaxCount, creds: Option[UserCredentials]): F[WriteResult] =
      setMaxCount(id, er, mc.some, creds)

    def removeMaxCount(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setMaxCount(id, er, None, creds)

    def setMaxCount(id: Id, er: StreamRevision, mc: Option[MaxCount], creds: Option[UserCredentials]): F[WriteResult] =
      modify(id, er, _.setMaxCount(mc), creds)

    //==================================================================================================================

    def getCacheControl(id: Id, creds: Option[UserCredentials]): F[Option[ReadResult[CacheControl]]] =
      getResult(id, _.cacheControl, creds)

    def setCacheControl(
      id: Id,
      er: StreamRevision,
      cacheControl: CacheControl,
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      setCacheControl(id, er, cacheControl.some, creds)

    def removeCacheControl(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setCacheControl(id, er, None, creds)

    def setCacheControl(
      id: Id,
      er: StreamRevision,
      cacheControl: Option[CacheControl],
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      modify(id, er, _.setCacheControl(cacheControl), creds)

    //==================================================================================================================

    def getAcl(id: Id, creds: Option[UserCredentials]): F[Option[ReadResult[StreamAcl]]] =
      getResult(id, _.acl, creds)

    def setAcl(id: Id, er: StreamRevision, acl: StreamAcl, creds: Option[UserCredentials]): F[WriteResult] =
      setAcl(id, er, acl.some, creds)

    def removeAcl(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setAcl(id, er, None, creds)

    def setAcl(id: Id, er: StreamRevision, acl: Option[StreamAcl], creds: Option[UserCredentials]): F[WriteResult] =
      modify(id, er, _.setAcl(acl), creds)

    //==================================================================================================================

    def getTruncateBefore(id: Id, creds: Option[UserCredentials]): F[Option[ReadResult[Exact]]] =
      getResult(id, _.truncateBefore, creds)

    def setTruncateBefore(
      id: Id,
      er: StreamRevision,
      truncateBefore: Exact,
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      setTruncateBefore(id, er, truncateBefore.some, creds)

    def removeTruncateBefore(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      setTruncateBefore(id, er, None, creds)

    def setTruncateBefore(
      id: Id,
      er: StreamRevision,
      truncateBefore: Option[Exact],
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      modify(id, er, _.setTruncateBefore(truncateBefore), creds)

    //==================================================================================================================

    def getCustom[T: Decoder](id: Id, creds: Option[UserCredentials]): F[Option[ReadResult[T]]] =
      getMetadata(id, creds) >>= { _.traverse(r => r.data.decodeCustom[F, T].map(r.withData)) }

    def setCustom[T: Codec.AsObject](
      id: Id,
      er: StreamRevision,
      custom: T,
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      modifyF(id, er, _.modifyCustom[F, T](_ => custom.some), creds)

    def removeCustom(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      modify(id, er, _.copy(custom = None), creds)

    //==================================================================================================================

    private[sec] def getResult[A](
      id: Id,
      fn: StreamMetadata => Option[A],
      creds: Option[UserCredentials]
    ): F[Option[ReadResult[A]]] =
      getMetadata(id, creds).nested.map(_.zoom(fn)).value

    private[sec] def getMetadata(id: Id, creds: Option[UserCredentials]): F[Option[MetaResult]] = {

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

    private[sec] def setMetadata(
      id: Id,
      er: StreamRevision,
      sm: StreamMetadata,
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      modify(id, er, _ => sm, creds: Option[UserCredentials])

    private[sec] def removeMetadata(id: Id, er: StreamRevision, creds: Option[UserCredentials]): F[WriteResult] =
      modify(id, er, _ => StreamMetadata.empty, creds)

    ///

    private[sec] def modify[A](
      id: Id,
      er: StreamRevision,
      mod: Endo[StreamMetadata],
      creds: Option[UserCredentials]
    ): F[WriteResult] =
      modifyF(id, er, mod(_).pure[F], creds)

    private[sec] def modifyF[A](
      id: Id,
      er: StreamRevision,
      mod: EndoF[F, StreamMetadata],
      creds: Option[UserCredentials]
    ): F[WriteResult] = {
      for {
        smr         <- getMetadata(id, creds)
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
        s.readStreamBackwards(mid, maxCount = 1, credentials = creds)
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
