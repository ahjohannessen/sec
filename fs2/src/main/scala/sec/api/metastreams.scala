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
import scodec.bits.ByteVector
import StreamId.Id
import StreamPosition.Exact
import StreamId.MetaId
import sec.api.exceptions.StreamNotFound
import sec.api.mapping._
import sec.syntax.all._
import MetaStreams._

trait MetaStreams[F[_]] {

  def getMaxAge(
    id: Id,
    creds: Option[UserCredentials]
  ): F[Option[ReadResult[MaxAge]]]

  def setMaxAge(
    id: Id,
    expectedState: StreamState,
    age: MaxAge,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def unsetMaxAge(
    id: Id,
    expectedState: StreamState,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def getMaxCount(
    id: Id,
    creds: Option[UserCredentials]
  ): F[Option[ReadResult[MaxCount]]]

  def setMaxCount(
    id: Id,
    expectedState: StreamState,
    count: MaxCount,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def unsetMaxCount(
    id: Id,
    expectedState: StreamState,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def getCacheControl(
    id: Id,
    creds: Option[UserCredentials]
  ): F[Option[ReadResult[CacheControl]]]

  def setCacheControl(
    id: Id,
    expectedState: StreamState,
    cacheControl: CacheControl,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def unsetCacheControl(
    id: Id,
    expectedState: StreamState,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def getAcl(
    id: Id,
    creds: Option[UserCredentials]
  ): F[Option[ReadResult[StreamAcl]]]

  def setAcl(
    id: Id,
    expectedState: StreamState,
    acl: StreamAcl,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def unsetAcl(
    id: Id,
    expectedState: StreamState,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def getTruncateBefore(
    id: Id,
    creds: Option[UserCredentials]
  ): F[Option[ReadResult[Exact]]]

  def setTruncateBefore(
    id: Id,
    expectedState: StreamState,
    truncateBefore: Exact,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def unsetTruncateBefore(
    id: Id,
    expectedState: StreamState,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def getCustom[T: Decoder](
    id: Id,
    creds: Option[UserCredentials]
  ): F[Option[ReadResult[T]]]

  def setCustom[T: Encoder.AsObject](
    id: Id,
    expectedState: StreamState,
    custom: T,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  def unsetCustom(
    id: Id,
    expectedState: StreamState,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  ///

  private[sec] def getMetadata(
    id: Id,
    creds: Option[UserCredentials]
  ): F[Option[MetaResult]]

  private[sec] def setMetadata(
    id: Id,
    expectedState: StreamState,
    data: StreamMetadata,
    creds: Option[UserCredentials]
  ): F[WriteResult]

  private[sec] def unsetMetadata(
    id: Id,
    expectedState: StreamState,
    creds: Option[UserCredentials]
  ): F[WriteResult]

}

object MetaStreams {

  private[sec] type EndoF[F[_], A] = A => F[A]

  //====================================================================================================================

  type ReadResult[T]           = Result[Option[T]]
  private[sec] type MetaResult = Result[StreamMetadata]

  final case class Result[T](
    streamPosition: StreamPosition.Exact,
    data: T
  )

  object Result {
    implicit final class ResultOps[A](val r: Result[A]) extends AnyVal {
      def withData[B](data: B): Result[B] = zoom(_ => data)
      def zoom[B](fn: A => B): Result[B]  = r.copy(data = fn(r.data))
    }
  }

  //====================================================================================================================

  private[sec] def apply[F[_]: Sync](s: Streams[F]): MetaStreams[F] = create[F](MetaRW[F](s))
  private[sec] def create[F[_]](meta: MetaRW[F])(implicit F: Sync[F]): MetaStreams[F] = new MetaStreams[F] {

    //==================================================================================================================

    def getMaxAge(id: Id, uc: Option[UserCredentials]): F[Option[ReadResult[MaxAge]]] =
      getResult(id, _.maxAge, uc)

    def setMaxAge(id: Id, es: StreamState, ma: MaxAge, uc: Option[UserCredentials]): F[WriteResult] =
      setMaxAge(id, es, ma.some, uc)

    def unsetMaxAge(id: Id, es: StreamState, uc: Option[UserCredentials]): F[WriteResult] =
      setMaxAge(id, es, None, uc)

    def setMaxAge(id: Id, es: StreamState, ma: Option[MaxAge], uc: Option[UserCredentials]): F[WriteResult] =
      modify(id, es, _.setMaxAge(ma), uc)

    //==================================================================================================================

    def getMaxCount(id: Id, uc: Option[UserCredentials]): F[Option[ReadResult[MaxCount]]] =
      getResult(id, _.maxCount, uc)

    def setMaxCount(id: Id, es: StreamState, mc: MaxCount, uc: Option[UserCredentials]): F[WriteResult] =
      setMaxCount(id, es, mc.some, uc)

    def unsetMaxCount(id: Id, expectedState: StreamState, uc: Option[UserCredentials]): F[WriteResult] =
      setMaxCount(id, expectedState, None, uc)

    def setMaxCount(id: Id, es: StreamState, mc: Option[MaxCount], uc: Option[UserCredentials]): F[WriteResult] =
      modify(id, es, _.setMaxCount(mc), uc)

    //==================================================================================================================

    def getCacheControl(id: Id, creds: Option[UserCredentials]): F[Option[ReadResult[CacheControl]]] =
      getResult(id, _.cacheControl, creds)

    def setCacheControl(id: Id,
                        expectedState: StreamState,
                        cc: CacheControl,
                        uc: Option[UserCredentials]): F[WriteResult] =
      setCControl(id, expectedState, cc.some, uc)

    def unsetCacheControl(id: Id, expectedState: StreamState, uc: Option[UserCredentials]): F[WriteResult] =
      setCControl(id, expectedState, None, uc)

    def setCControl(id: Id, es: StreamState, cc: Option[CacheControl], uc: Option[UserCredentials]): F[WriteResult] =
      modify(id, es, _.setCacheControl(cc), uc)

    //==================================================================================================================

    def getAcl(id: Id, uc: Option[UserCredentials]): F[Option[ReadResult[StreamAcl]]] =
      getResult(id, _.acl, uc)

    def setAcl(id: Id, expectedState: StreamState, acl: StreamAcl, uc: Option[UserCredentials]): F[WriteResult] =
      setAcl(id, expectedState, acl.some, uc)

    def unsetAcl(id: Id, expectedState: StreamState, uc: Option[UserCredentials]): F[WriteResult] =
      setAcl(id, expectedState, None, uc)

    def setAcl(id: Id, es: StreamState, acl: Option[StreamAcl], uc: Option[UserCredentials]): F[WriteResult] =
      modify(id, es, _.setAcl(acl), uc)

    //==================================================================================================================

    def getTruncateBefore(id: Id, uc: Option[UserCredentials]): F[Option[ReadResult[Exact]]] =
      getResult(id, _.truncateBefore, uc)

    def setTruncateBefore(id: Id, expectedState: StreamState, tb: Exact, uc: Option[UserCredentials]): F[WriteResult] =
      setTruncateBefore(id, expectedState, tb.some, uc)

    def unsetTruncateBefore(id: Id, expectedState: StreamState, uc: Option[UserCredentials]): F[WriteResult] =
      setTruncateBefore(id, expectedState, None, uc)

    def setTruncateBefore(id: Id, es: StreamState, tb: Option[Exact], uc: Option[UserCredentials]): F[WriteResult] =
      modify(id, es, _.setTruncateBefore(tb), uc)

    //==================================================================================================================

    def getCustom[T: Decoder](id: Id, uc: Option[UserCredentials]): F[Option[ReadResult[T]]] =
      getMetadata(id, uc) >>= { _.traverse(r => r.data.getCustom[F, T].map(r.withData)) }

    def setCustom[T: Encoder.AsObject](id: Id,
                                       expectedState: StreamState,
                                       c: T,
                                       uc: Option[UserCredentials]): F[WriteResult] =
      modify(id, expectedState, _.setCustom[T](c), uc)

    def unsetCustom(id: Id, expectedState: StreamState, uc: Option[UserCredentials]): F[WriteResult] =
      modify(id, expectedState, _.copy(custom = None), uc)

    //==================================================================================================================

    private[sec] def getResult[A](
      id: Id,
      fn: StreamMetadata => Option[A],
      uc: Option[UserCredentials]
    ): F[Option[ReadResult[A]]] =
      getMetadata(id, uc).nested.map(_.zoom(fn)).value

    private[sec] def getMetadata(id: Id, uc: Option[UserCredentials]): F[Option[MetaResult]] = {

      val decodeJson: EventRecord => F[StreamMetadata] =
        _.eventData.data.decodeUtf8.leftMap(DecodingError(_)).liftTo[F] >>= { utf8 =>
          decode[StreamMetadata](utf8).leftMap(DecodingError(_)).liftTo[F]
        }

      val recoverRead: PartialFunction[Throwable, Option[EventRecord]] = { case _: StreamNotFound =>
        none[EventRecord]
      }

      meta.read(id.metaId, uc).recover(recoverRead) >>= {
        _.traverse(es => decodeJson(es).map(Result(es.streamPosition, _)))
      }

    }

    private[sec] def setMetadata(
      id: Id,
      expectedState: StreamState,
      sm: StreamMetadata,
      uc: Option[UserCredentials]
    ): F[WriteResult] =
      modify(id, expectedState, _ => sm, uc)

    private[sec] def unsetMetadata(id: Id, expectedState: StreamState, uc: Option[UserCredentials]): F[WriteResult] =
      modify(id, expectedState, _ => StreamMetadata.empty, uc)

    ///

    private[sec] def modify[A](
      id: Id,
      es: StreamState,
      mod: Endo[StreamMetadata],
      uc: Option[UserCredentials]
    ): F[WriteResult] =
      modifyF(id, es, mod(_).pure[F], uc)

    private[sec] def modifyF[A](
      id: Id,
      es: StreamState,
      mod: EndoF[F, StreamMetadata],
      uc: Option[UserCredentials]
    ): F[WriteResult] = {
      for {
        smr         <- getMetadata(id, uc)
        modified    <- mod(smr.fold(StreamMetadata.empty)(_.data))
        eid         <- uuid[F]
        eventData   <- mkEventData[F](eid, modified)
        writeResult <- meta.write(id.metaId, es, eventData, uc)
      } yield writeResult
    }

  }

  ///

  private[sec] val printer: Printer             = Printer.noSpaces.copy(dropNullValues = true)
  private[sec] def uuid[F[_]: Sync]: F[ju.UUID] = Sync[F].delay(ju.UUID.randomUUID())

  private[sec] def mkEventData[F[_]: ErrorA](eventId: ju.UUID, sm: StreamMetadata): F[EventData] =
    ByteVector
      .encodeUtf8(printer.print(Encoder[StreamMetadata].apply(sm)))
      .map(EventData(EventType.StreamMetadata, eventId, _, ContentType.Json))
      .liftTo[F]

  ///

  private[sec] trait MetaRW[F[_]] {
    def read(mid: MetaId, uc: Option[UserCredentials]): F[Option[EventRecord]]
    def write(mid: MetaId, es: StreamState, data: EventData, uc: Option[UserCredentials]): F[WriteResult]
  }

  private[sec] object MetaRW {

    def apply[F[_]](s: Streams[F])(implicit F: Sync[F]): MetaRW[F] = new MetaRW[F] {

      def read(mid: MetaId, uc: Option[UserCredentials]): F[Option[EventRecord]] =
        s.readStreamBackwards(mid, maxCount = 1, credentials = uc).collect { case er: EventRecord => er }.compile.last

      def write(mid: MetaId, es: StreamState, data: EventData, uc: Option[UserCredentials]): F[WriteResult] =
        s.appendToStream(mid, es, NonEmptyList.one(data), uc)

    }
  }

}
