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
import StreamId.Id

trait MetaStreamsSyntax {

  extension [F[_]: ErrorM](ms: MetaStreams[F]) {

  def setMaxAge(id: Id, expectedState: StreamState, age: FiniteDuration): F[WriteResult] =
    MaxAge(age).liftTo[F] >>= (ms.setMaxAge(id, expectedState, _))

  def setMaxCount(id: Id, expectedState: StreamState, count: Int): F[WriteResult] =
    MaxCount(count).liftTo[F] >>= (ms.setMaxCount(id, expectedState, _))

  def setCacheControl(id: Id, expectedState: StreamState, cacheControl: FiniteDuration): F[WriteResult] =
    CacheControl(cacheControl).liftTo[F] >>= (ms.setCacheControl(id, expectedState, _))

  def setTruncateBefore(id: Id, expectedState: StreamState, truncateBefore: Long): F[WriteResult] =
    StreamPosition(truncateBefore).liftTo[F] >>= (ms.setTruncateBefore(id, expectedState, _))

  }

}