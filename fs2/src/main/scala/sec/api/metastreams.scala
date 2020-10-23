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

  def getMaxAge(id: Id): F[Option[ReadResult[MaxAge]]]
  def setMaxAge(id: Id, expectedState: StreamState, age: MaxAge): F[WriteResult]
  def unsetMaxAge(id: Id, expectedState: StreamState): F[WriteResult]

  def getMaxCount(id: Id): F[Option[ReadResult[MaxCount]]]
  def setMaxCount(id: Id, expectedState: StreamState, count: MaxCount): F[WriteResult]
  def unsetMaxCount(id: Id, expectedState: StreamState): F[WriteResult]

  def getCacheControl(id: Id): F[Option[ReadResult[CacheControl]]]
  def setCacheControl(id: Id, expectedState: StreamState, cacheControl: CacheControl): F[WriteResult]
  def unsetCacheControl(id: Id, expectedState: StreamState): F[WriteResult]

  def getAcl(id: Id): F[Option[ReadResult[StreamAcl]]]
  def setAcl(id: Id, expectedState: StreamState, acl: StreamAcl): F[WriteResult]
  def unsetAcl(id: Id, expectedState: StreamState): F[WriteResult]

  def getTruncateBefore(id: Id): F[Option[ReadResult[Exact]]]
  def setTruncateBefore(id: Id, expectedState: StreamState, truncateBefore: Exact): F[WriteResult]
  def unsetTruncateBefore(id: Id, expectedState: StreamState): F[WriteResult]

  def getCustom[T: Decoder](id: Id): F[Option[ReadResult[T]]]
  def setCustom[T: Encoder.AsObject](id: Id, expectedState: StreamState, custom: T): F[WriteResult]
  def unsetCustom(id: Id, expectedState: StreamState): F[WriteResult]

  def withCredentials(creds: UserCredentials): MetaStreams[F]

  ///

  private[sec] def getMetadata(id: Id): F[Option[MetaResult]]
  private[sec] def setMetadata(id: Id, expectedState: StreamState, data: StreamMetadata): F[WriteResult]
  private[sec] def unsetMetadata(id: Id, expectedState: StreamState): F[WriteResult]

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

  private[sec] def apply[F[_]: Sync](s: Streams[F]): MetaStreams[F] = MetaStreams[F](MetaRW[F](s))
  private[sec] def apply[F[_]](meta: MetaRW[F])(implicit F: Sync[F]): MetaStreams[F] = new MetaStreams[F] {

    //==================================================================================================================

    def getMaxAge(id: Id): F[Option[ReadResult[MaxAge]]]               = getResult(id, _.maxAge)
    def setMaxAge(id: Id, es: StreamState, ma: MaxAge): F[WriteResult] = setMaxAge(id, es, ma.some)
    def unsetMaxAge(id: Id, es: StreamState): F[WriteResult]           = setMaxAge(id, es, None)

    def setMaxAge(id: Id, es: StreamState, ma: Option[MaxAge]): F[WriteResult] =
      modify(id, es, _.setMaxAge(ma))

    //==================================================================================================================

    def getMaxCount(id: Id): F[Option[ReadResult[MaxCount]]]               = getResult(id, _.maxCount)
    def setMaxCount(id: Id, es: StreamState, mc: MaxCount): F[WriteResult] = setMaxCount(id, es, mc.some)
    def unsetMaxCount(id: Id, expectedState: StreamState): F[WriteResult]  = setMaxCount(id, expectedState, None)

    def setMaxCount(id: Id, es: StreamState, mc: Option[MaxCount]): F[WriteResult] =
      modify(id, es, _.setMaxCount(mc))

    //==================================================================================================================

    def getCacheControl(id: Id): F[Option[ReadResult[CacheControl]]]               = getResult(id, _.cacheControl)
    def setCacheControl(id: Id, es: StreamState, cc: CacheControl): F[WriteResult] = setCControl(id, es, cc.some)
    def unsetCacheControl(id: Id, es: StreamState): F[WriteResult]                 = setCControl(id, es, None)

    def setCControl(id: Id, es: StreamState, cc: Option[CacheControl]): F[WriteResult] =
      modify(id, es, _.setCacheControl(cc))

    //==================================================================================================================

    def getAcl(id: Id): F[Option[ReadResult[StreamAcl]]]                        = getResult(id, _.acl)
    def setAcl(id: Id, es: StreamState, acl: StreamAcl): F[WriteResult]         = setAcl(id, es, acl.some)
    def unsetAcl(id: Id, es: StreamState): F[WriteResult]                       = setAcl(id, es, None)
    def setAcl(id: Id, es: StreamState, acl: Option[StreamAcl]): F[WriteResult] = modify(id, es, _.setAcl(acl))

    //==================================================================================================================

    def getTruncateBefore(id: Id): F[Option[ReadResult[Exact]]]               = getResult(id, _.truncateBefore)
    def setTruncateBefore(id: Id, es: StreamState, tb: Exact): F[WriteResult] = setTruncateBefore(id, es, tb.some)
    def unsetTruncateBefore(id: Id, es: StreamState): F[WriteResult]          = setTruncateBefore(id, es, None)

    def setTruncateBefore(id: Id, es: StreamState, tb: Option[Exact]): F[WriteResult] =
      modify(id, es, _.setTruncateBefore(tb))

    //==================================================================================================================

    def getCustom[T: Decoder](id: Id): F[Option[ReadResult[T]]] =
      getMetadata(id) >>= { _.traverse(r => r.data.getCustom[F, T].map(r.withData)) }

    def setCustom[T: Encoder.AsObject](id: Id, es: StreamState, c: T): F[WriteResult] =
      modify(id, es, _.setCustom[T](c))

    def unsetCustom(id: Id, es: StreamState): F[WriteResult] =
      modify(id, es, _.copy(custom = None))

    //==================================================================================================================

    def withCredentials(creds: UserCredentials): MetaStreams[F] =
      MetaStreams[F](meta.withCredentials(creds))

    //==================================================================================================================

    private[sec] def getResult[A](id: Id, fn: StreamMetadata => Option[A]): F[Option[ReadResult[A]]] =
      getMetadata(id).nested.map(_.zoom(fn)).value

    private[sec] def getMetadata(id: Id): F[Option[MetaResult]] = {

      val decodeJson: EventRecord => F[StreamMetadata] =
        _.eventData.data.decodeUtf8.leftMap(DecodingError(_)).liftTo[F] >>= { utf8 =>
          decode[StreamMetadata](utf8).leftMap(DecodingError(_)).liftTo[F]
        }

      val recoverRead: PartialFunction[Throwable, Option[EventRecord]] = { case _: StreamNotFound =>
        none[EventRecord]
      }

      meta.read(id.metaId).recover(recoverRead) >>= {
        _.traverse(es => decodeJson(es).map(Result(es.streamPosition, _)))
      }

    }

    private[sec] def setMetadata(id: Id, expectedState: StreamState, sm: StreamMetadata): F[WriteResult] =
      modify(id, expectedState, _ => sm)

    private[sec] def unsetMetadata(id: Id, expectedState: StreamState): F[WriteResult] =
      modify(id, expectedState, _ => StreamMetadata.empty)

    ///

    private[sec] def modify[A](id: Id, es: StreamState, mod: Endo[StreamMetadata]): F[WriteResult] =
      modifyF(id, es, mod(_).pure[F])

    private[sec] def modifyF[A](id: Id, es: StreamState, mod: EndoF[F, StreamMetadata]): F[WriteResult] = {
      for {
        smr         <- getMetadata(id)
        modified    <- mod(smr.fold(StreamMetadata.empty)(_.data))
        eid         <- uuid[F]
        eventData   <- mkEventData[F](eid, modified)
        writeResult <- meta.write(id.metaId, es, eventData)
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
    def read(mid: MetaId): F[Option[EventRecord]]
    def write(mid: MetaId, es: StreamState, data: EventData): F[WriteResult]
    def withCredentials(creds: UserCredentials): MetaRW[F]
  }

  private[sec] object MetaRW {

    def apply[F[_]](s: Streams[F])(implicit F: Sync[F]): MetaRW[F] = new MetaRW[F] {

      def withCredentials(creds: UserCredentials): MetaRW[F] =
        MetaRW[F](s.withCredentials(creds))

      def read(mid: MetaId): F[Option[EventRecord]] =
        s.readStreamBackwards(mid, maxCount = 1).collect { case er: EventRecord => er }.compile.last

      def write(mid: MetaId, es: StreamState, data: EventData): F[WriteResult] =
        s.appendToStream(mid, es, NonEmptyList.one(data))

    }
  }

}
