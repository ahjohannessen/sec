/*
 * Copyright 2020 Scala EventStoreDB Client
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
import sec.api.exceptions.StreamNotFound
import sec.api.mapping._
import sec.syntax.all._

import StreamId.Id
import StreamPosition.Exact
import StreamId.MetaId
import MetaStreams._

/** API for interacting with metadata streams in EventStoreDB.
  *
  * Methods for getting, setting and unsetting metadata for streams.
  *
  * @tparam F
  *   the effect type in which [[MetaStreams]] operates.
  */
trait MetaStreams[F[_]] {

  /** Gets the max age for a stream.
    *
    * @param id
    *   the id of the stream.
    * @return
    *   an optional result for the metadata stream containing current [[StreamPosition]] for the metadata stream and the
    *   [[MaxAge]] value.
    */
  def getMaxAge(id: Id): F[Option[ReadResult[MaxAge]]]

  /** Sets [[MaxAge]] for a stream and returns [[WriteResult]] with current positions of the stream after a successful
    * operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    * @param age
    *   the max age for data in the stream.
    */
  def setMaxAge(id: Id, expectedState: StreamState, age: MaxAge): F[WriteResult]

  /** Removes [[MaxAge]] for a stream and returns [[WriteResult]] with current positions of the stream after a
    * successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    */
  def unsetMaxAge(id: Id, expectedState: StreamState): F[WriteResult]

  /** Gets the max count for a stream.
    *
    * @param id
    *   the id of the stream.
    * @return
    *   an optional result for the metadata stream containing current [[StreamPosition]] for the metadata stream and the
    *   [[MaxCount]] value.
    */
  def getMaxCount(id: Id): F[Option[ReadResult[MaxCount]]]

  /** Sets [[MaxCount]] for a stream and returns [[WriteResult]] with current positions of the stream after a successful
    * operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    * @param count
    *   the max count of data in the stream.
    */
  def setMaxCount(id: Id, expectedState: StreamState, count: MaxCount): F[WriteResult]

  /** Removes [[MaxCount]] for a stream and returns [[WriteResult]] with current positions of the stream after a
    * successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    */
  def unsetMaxCount(id: Id, expectedState: StreamState): F[WriteResult]

  /** Gets the cache control for a stream.
    *
    * @param id
    *   the id of the stream.
    * @return
    *   an optional result for the metadata stream containing current [[StreamPosition]] for the metadata stream and the
    *   [[CacheControl]] value.
    */
  def getCacheControl(id: Id): F[Option[ReadResult[CacheControl]]]

  /** Sets [[CacheControl]] for a stream and returns [[WriteResult]] with current positions of the stream after a
    * successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    * @param cacheControl
    *   the cache control for the stream.
    */
  def setCacheControl(id: Id, expectedState: StreamState, cacheControl: CacheControl): F[WriteResult]

  /** Removes [[CacheControl]] for a stream and returns [[WriteResult]] with current positions of the stream after a
    * successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    */
  def unsetCacheControl(id: Id, expectedState: StreamState): F[WriteResult]

  /** Gets the access control list for a stream.
    *
    * @param id
    *   the id of the stream.
    * @return
    *   an optional result for the metadata stream containing current [[StreamPosition]] for the metadata stream and the
    *   [[StreamAcl]] value.
    */
  def getAcl(id: Id): F[Option[ReadResult[StreamAcl]]]

  /** Sets [[StreamAcl]] for a stream and returns [[WriteResult]] with current positions of the stream after a
    * successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    * @param acl
    *   the access control list for the stream.
    */
  def setAcl(id: Id, expectedState: StreamState, acl: StreamAcl): F[WriteResult]

  /** Removes [[StreamAcl]] for a stream and returns [[WriteResult]] with current positions of the stream after a
    * successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    */
  def unsetAcl(id: Id, expectedState: StreamState): F[WriteResult]

  /** Gets the [[StreamPosition]] value that a stream is truncated before.
    *
    * @param id
    *   the id of the stream.
    * @return
    *   an optional result for the metadata stream containing current [[StreamPosition]] for the metadata stream and the
    *   [[StreamPosition]] truncate before value.
    */
  def getTruncateBefore(id: Id): F[Option[ReadResult[Exact]]]

  /** Sets [[StreamPosition]] truncated value for a stream and returns [[WriteResult]] with current positions of the
    * stream after a successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    * @param truncateBefore
    *   the truncated before stream position for the stream, the value used entails that events with a stream position
    *   less than the truncated before value should be removed.
    */
  def setTruncateBefore(id: Id, expectedState: StreamState, truncateBefore: Exact): F[WriteResult]

  /** Removes [[StreamPosition]] truncated value for a stream and returns [[WriteResult]] with current positions of the
    * stream after a successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    */
  def unsetTruncateBefore(id: Id, expectedState: StreamState): F[WriteResult]

  /** Gets a custom JSON encoded metadata value for a stream using a provided decoder.
    *
    * @param id
    *   the id of the stream.
    * @return
    *   an optional result for the metadata stream containing current [[StreamPosition]] for the metadata stream and a
    *   JSON value using the provided [[io.circe.Decoder]] decoder.
    */
  def getCustom[T: Decoder](id: Id): F[Option[ReadResult[T]]]

  /** Sets a custom JSON metadata value for a stream and returns [[WriteResult]] with current positions of the stream
    * after a successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    * @param custom
    *   the custom JSON value using the provided [[io.circe.Encoder.AsObject]] encoder.
    */
  def setCustom[T: Encoder.AsObject](id: Id, expectedState: StreamState, custom: T): F[WriteResult]

  /** Removes custom JSON metadata value for a stream and returns [[WriteResult]] with current positions of the stream
    * after a successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @note
    *   Removing custom JSON metadata does not affect other metadata values.
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    */
  def unsetCustom(id: Id, expectedState: StreamState): F[WriteResult]

  /** Returns an instance that uses provided [[UserCredentials]]. This is useful when an operation requires different
    * credentials from what is provided through configuration.
    *
    * @param creds
    *   Custom user credentials to use.
    */
  def withCredentials(creds: UserCredentials): MetaStreams[F]

  ///

  private[sec] def getMetadata(id: Id): F[Option[MetaResult]]
  private[sec] def setMetadata(id: Id, expectedState: StreamState, data: StreamMetadata): F[WriteResult]
  private[sec] def unsetMetadata(id: Id, expectedState: StreamState): F[WriteResult]

}

object MetaStreams {

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

    def getMaxCount(id: Id): F[Option[ReadResult[MaxCount]]]               = getResult(id, _.maxCount)
    def setMaxCount(id: Id, es: StreamState, mc: MaxCount): F[WriteResult] = setMaxCount(id, es, mc.some)
    def unsetMaxCount(id: Id, expectedState: StreamState): F[WriteResult]  = setMaxCount(id, expectedState, None)
    def setMaxCount(id: Id, es: StreamState, mc: Option[MaxCount]): F[WriteResult] =
      modify(id, es, _.setMaxCount(mc))

    def getCacheControl(id: Id): F[Option[ReadResult[CacheControl]]]               = getResult(id, _.cacheControl)
    def setCacheControl(id: Id, es: StreamState, cc: CacheControl): F[WriteResult] = setCControl(id, es, cc.some)
    def unsetCacheControl(id: Id, es: StreamState): F[WriteResult]                 = setCControl(id, es, None)
    def setCControl(id: Id, es: StreamState, cc: Option[CacheControl]): F[WriteResult] =
      modify(id, es, _.setCacheControl(cc))

    def getAcl(id: Id): F[Option[ReadResult[StreamAcl]]]                        = getResult(id, _.acl)
    def setAcl(id: Id, es: StreamState, acl: StreamAcl): F[WriteResult]         = setAcl(id, es, acl.some)
    def unsetAcl(id: Id, es: StreamState): F[WriteResult]                       = setAcl(id, es, None)
    def setAcl(id: Id, es: StreamState, acl: Option[StreamAcl]): F[WriteResult] = modify(id, es, _.setAcl(acl))

    def getTruncateBefore(id: Id): F[Option[ReadResult[Exact]]]               = getResult(id, _.truncateBefore)
    def setTruncateBefore(id: Id, es: StreamState, tb: Exact): F[WriteResult] = setTruncateBefore(id, es, tb.some)
    def unsetTruncateBefore(id: Id, es: StreamState): F[WriteResult]          = setTruncateBefore(id, es, None)
    def setTruncateBefore(id: Id, es: StreamState, tb: Option[Exact]): F[WriteResult] =
      modify(id, es, _.setTruncateBefore(tb))

    def getCustom[T: Decoder](id: Id): F[Option[ReadResult[T]]] =
      getMetadata(id) >>= { _.traverse(r => r.data.getCustom[F, T].map(r.withData)) }

    def setCustom[T: Encoder.AsObject](id: Id, es: StreamState, c: T): F[WriteResult] =
      modify(id, es, _.setCustom[T](c))

    def unsetCustom(id: Id, es: StreamState): F[WriteResult] =
      modify(id, es, _.copy(custom = None))

    def withCredentials(creds: UserCredentials): MetaStreams[F] =
      MetaStreams[F](meta.withCredentials(creds))

    //==================================================================================================================

    private[sec] def getResult[A](id: Id, fn: StreamMetadata => Option[A]): F[Option[ReadResult[A]]] =
      getMetadata(id).nested.map(_.zoom(fn)).value

    private[sec] def getMetadata(id: Id): F[Option[MetaResult]] = {

      val decodeJson: EventData => F[StreamMetadata] =
        _.data.decodeUtf8.leftMap(DecodingError(_)).liftTo[F] >>= { utf8 =>
          decode[StreamMetadata](utf8).leftMap(DecodingError(_)).liftTo[F]
        }

      val recoverRead: PartialFunction[Throwable, Option[EventRecord[PositionInfo.Local]]] = { case _: StreamNotFound =>
        none[EventRecord[PositionInfo.Local]]
      }

      meta.read(id.metaId).recover(recoverRead) >>= {
        _.traverse(es => decodeJson(es.eventData).map(Result(es.streamPosition, _)))
      }

    }

    private[sec] def setMetadata(id: Id, expectedState: StreamState, sm: StreamMetadata): F[WriteResult] =
      modify(id, expectedState, _ => sm)

    private[sec] def unsetMetadata(id: Id, expectedState: StreamState): F[WriteResult] =
      modify(id, expectedState, _ => StreamMetadata.empty)

    private[sec] def modify[A](id: Id, es: StreamState, mod: Endo[StreamMetadata]): F[WriteResult] =
      modifyF(id, es, mod(_).pure[F])

    private[sec] def modifyF[A](id: Id, es: StreamState, mod: StreamMetadata => F[StreamMetadata]): F[WriteResult] = {
      for {
        smr         <- getMetadata(id)
        modified    <- mod(smr.fold(StreamMetadata.empty)(_.data))
        eid         <- uuid[F]
        eventData   <- mkEventData[F](eid, modified)
        writeResult <- meta.write(id.metaId, es, eventData)
      } yield writeResult
    }

  }

  private[sec] val printer: Printer             = Printer.noSpaces.copy(dropNullValues = true)
  private[sec] def uuid[F[_]: Sync]: F[ju.UUID] = Sync[F].delay(ju.UUID.randomUUID())

  private[sec] def mkEventData[F[_]: ErrorA](eventId: ju.UUID, sm: StreamMetadata): F[EventData] =
    ByteVector
      .encodeUtf8(printer.print(Encoder[StreamMetadata].apply(sm)))
      .map(EventData(EventType.StreamMetadata, eventId, _, ContentType.Json))
      .liftTo[F]

  private[sec] trait MetaRW[F[_]] {
    def read(mid: MetaId): F[Option[EventRecord[PositionInfo.Local]]]
    def write(mid: MetaId, es: StreamState, data: EventData): F[WriteResult]
    def withCredentials(creds: UserCredentials): MetaRW[F]
  }

  private[sec] object MetaRW {

    def apply[F[_]](s: Streams[F])(implicit F: Sync[F]): MetaRW[F] = new MetaRW[F] {

      def withCredentials(creds: UserCredentials): MetaRW[F] =
        MetaRW[F](s.withCredentials(creds))

      def read(mid: MetaId): F[Option[EventRecord[PositionInfo.Local]]] =
        s.readStreamBackwards(mid, maxCount = 1)
          .collect { case er: EventRecord[PositionInfo.Local] => er }
          .compile
          .last

      def write(mid: MetaId, es: StreamState, data: EventData): F[WriteResult] =
        s.appendToStream(mid, es, NonEmptyList.one(data))

    }
  }

}
