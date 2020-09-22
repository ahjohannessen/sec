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
import sec.core._
import sec.core.EventNumber.Exact
import sec.core.StreamId.Id

object api {

  implicit def syntaxForMetaStreams[F[_]: ErrorM](ms: MetaStreams[F]): MetaStreamsSyntax[F] =
    new MetaStreamsSyntax[F](ms)

//====================================================================================================================

  final class MetaStreamsSyntax[F[_]: ErrorM](val ms: MetaStreams[F]) {

    def getMaxAge(id: Id): F[Option[ReadResult[MaxAge]]] =
      ms.getMaxAge(id, None)

    def setMaxAge(id: Id, expectedRevision: StreamRevision, age: FiniteDuration): F[WriteResult] =
      MaxAge.lift[F](age) >>= (ms.setMaxAge(id, expectedRevision, _, None))

    def removeMaxAge(id: Id, expectedRevision: StreamRevision): F[WriteResult] =
      ms.removeMaxAge(id, expectedRevision, None)

    def getMaxCount(id: Id): F[Option[ReadResult[MaxCount]]] =
      ms.getMaxCount(id, None)

    def setMaxCount(id: Id, expectedRevision: StreamRevision, count: Int): F[WriteResult] =
      MaxCount.lift[F](count) >>= (ms.setMaxCount(id, expectedRevision, _, None))

    def removeMaxCount(id: Id, expectedRevision: StreamRevision): F[WriteResult] =
      ms.removeMaxCount(id, expectedRevision, None)

    def getCacheControl(id: Id): F[Option[ReadResult[CacheControl]]] =
      ms.getCacheControl(id, None)

    def setCacheControl(id: Id, expectedRevision: StreamRevision, cacheControl: FiniteDuration): F[WriteResult] =
      CacheControl.lift[F](cacheControl) >>= (ms.setCacheControl(id, expectedRevision, _, None))

    def removeCacheControl(id: Id, expectedRevision: StreamRevision): F[MetaStreams.WriteResult] =
      ms.removeCacheControl(id, expectedRevision, None)

    def getAcl(id: Id): F[Option[ReadResult[StreamAcl]]] =
      ms.getAcl(id, None)

    def setAcl(id: Id, expectedRevision: StreamRevision, acl: StreamAcl): F[WriteResult] =
      ms.setAcl(id, expectedRevision, acl, None)

    def removeAcl(id: Id, expectedRevision: StreamRevision): F[WriteResult] =
      ms.removeAcl(id, expectedRevision, None)

    def getTruncateBefore(id: Id): F[Option[ReadResult[Exact]]] =
      ms.getTruncateBefore(id, None)

    def setTruncateBefore(id: Id, expectedRevision: StreamRevision, truncateBefore: Long): F[WriteResult] =
      EventNumber.Exact.lift[F](truncateBefore) >>= (ms.setTruncateBefore(id, expectedRevision, _, None))

    def removeTruncateBefore(id: Id, expectedRevision: StreamRevision): F[WriteResult] =
      ms.removeTruncateBefore(id, expectedRevision, None)

    ///

    private[sec] def getMetadata(id: Id): F[Option[MetaResult]] =
      ms.getMetadata(id, None)

    private[sec] def setMetadata(id: Id, expectedRevision: StreamRevision, data: StreamMetadata): F[WriteResult] =
      ms.setMetadata(id, expectedRevision, data, None)

    private[sec] def removeMetadata(id: Id, expectedRevision: StreamRevision): F[WriteResult] =
      ms.removeMetadata(id, expectedRevision, None)
  }

//====================================================================================================================

}
