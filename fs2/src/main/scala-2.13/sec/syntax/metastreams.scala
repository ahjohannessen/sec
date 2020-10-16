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
package syntax

import scala.concurrent.duration.FiniteDuration
import cats.syntax.all._
import sec.api._
import sec.api.MetaStreams._
import StreamPosition.Exact
import StreamId.Id

//====================================================================================================================

trait MetaStreamsSyntax {

  implicit final def syntaxForMetaStreams[F[_]: ErrorM](ms: MetaStreams[F]): MetaStreamsOps[F] =
    new MetaStreamsOps[F](ms)
}

//====================================================================================================================

final class MetaStreamsOps[F[_]: ErrorM](val ms: MetaStreams[F]) {

  def getMaxAge(id: Id): F[Option[ReadResult[MaxAge]]] =
    ms.getMaxAge(id, None)

  def setMaxAge(id: Id, expectedState: StreamState, age: FiniteDuration): F[WriteResult] =
    setMaxAgeF(id, expectedState, age, None)

  def setMaxAge(id: Id, expectedState: StreamState, age: FiniteDuration, uc: UserCredentials): F[WriteResult] =
    setMaxAgeF(id, expectedState, age, uc.some)

  private def setMaxAgeF(id: Id, er: StreamState, age: FiniteDuration, uc: Option[UserCredentials]): F[WriteResult] =
    MaxAge(age).liftTo[F] >>= (ms.setMaxAge(id, er, _, uc))

  def unsetMaxAge(id: Id, expectedState: StreamState): F[WriteResult] =
    ms.unsetMaxAge(id, expectedState, None)

  def getMaxCount(id: Id): F[Option[ReadResult[MaxCount]]] =
    ms.getMaxCount(id, None)

  def setMaxCount(id: Id, expectedState: StreamState, count: Int): F[WriteResult] =
    setMaxCountF(id, expectedState, count, None)

  def setMaxCount(id: Id, expectedState: StreamState, count: Int, uc: UserCredentials): F[WriteResult] =
    setMaxCountF(id, expectedState, count, uc.some)

  private def setMaxCountF(id: Id, er: StreamState, count: Int, uc: Option[UserCredentials]): F[WriteResult] =
    MaxCount(count).liftTo[F] >>= (ms.setMaxCount(id, er, _, uc))

  def unsetMaxCount(id: Id, expectedState: StreamState): F[WriteResult] =
    ms.unsetMaxCount(id, expectedState, None)

  def getCacheControl(id: Id): F[Option[ReadResult[CacheControl]]] =
    ms.getCacheControl(id, None)

  def setCacheControl(id: Id, expectedState: StreamState, cacheControl: FiniteDuration): F[WriteResult] =
    setCacheControlF(id, expectedState, cacheControl, None)

  def setCacheControl(
    id: Id,
    expectedState: StreamState,
    cacheControl: FiniteDuration,
    uc: UserCredentials
  ): F[WriteResult] =
    setCacheControlF(id, expectedState, cacheControl, uc.some)

  private def setCacheControlF(
    id: Id,
    er: StreamState,
    cc: FiniteDuration,
    uc: Option[UserCredentials]
  ): F[WriteResult] =
    CacheControl(cc).liftTo[F] >>= (ms.setCacheControl(id, er, _, uc))

  def unsetCacheControl(id: Id, expectedState: StreamState): F[WriteResult] =
    ms.unsetCacheControl(id, expectedState, None)

  def getAcl(id: Id): F[Option[ReadResult[StreamAcl]]] =
    ms.getAcl(id, None)

  def setAcl(id: Id, expectedState: StreamState, acl: StreamAcl): F[WriteResult] =
    ms.setAcl(id, expectedState, acl, None)

  def unsetAcl(id: Id, expectedState: StreamState): F[WriteResult] =
    ms.unsetAcl(id, expectedState, None)

  def getTruncateBefore(id: Id): F[Option[ReadResult[Exact]]] =
    ms.getTruncateBefore(id, None)

  def setTruncateBefore(id: Id, expectedState: StreamState, truncateBefore: Long): F[WriteResult] =
    setTruncateBeforeF(id, expectedState, truncateBefore, None)

  def setTruncateBefore(
    id: Id,
    expectedState: StreamState,
    truncateBefore: Long,
    uc: UserCredentials
  ): F[WriteResult] =
    setTruncateBeforeF(id, expectedState, truncateBefore, uc.some)

  private def setTruncateBeforeF(id: Id, er: StreamState, tb: Long, uc: Option[UserCredentials]): F[WriteResult] =
    StreamPosition(tb).liftTo[F] >>= (ms.setTruncateBefore(id, er, _, uc))

  def unsetTruncateBefore(id: Id, expectedState: StreamState): F[WriteResult] =
    ms.unsetTruncateBefore(id, expectedState, None)

  ///

  private[sec] def getMetadata(id: Id): F[Option[MetaResult]] =
    ms.getMetadata(id, None)

  private[sec] def setMetadata(id: Id, expectedState: StreamState, data: StreamMetadata): F[WriteResult] =
    ms.setMetadata(id, expectedState, data, None)

  private[sec] def unsetMetadata(id: Id, expectedState: StreamState): F[WriteResult] =
    ms.unsetMetadata(id, expectedState, None)
}

//====================================================================================================================
