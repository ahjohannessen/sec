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
package syntax

import scala.concurrent.duration.FiniteDuration
import cats.MonadThrow
import cats.syntax.all._
import sec.api._

import StreamId.Id

trait MetaStreamsSyntax {

  implicit final def syntaxForMetaStreams[F[_]: MonadThrow](ms: MetaStreams[F]): MetaStreamsOps[F] =
    new MetaStreamsOps[F](ms)
}

final class MetaStreamsOps[F[_]: MonadThrow](val ms: MetaStreams[F]) {

  /** Sets max age in [[scala.concurrent.duration.FiniteDuration]] for a stream and returns [[sec.api.WriteResult]] with
    * current positions of the stream after a successful operation. Failure to fulfill the expected state is manifested
    * by raising [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    * @param age
    *   the max age [[scala.concurrent.duration.FiniteDuration]] value for data in the stream. Valid values are
    *   [[scala.concurrent.duration.FiniteDuration]] greater or equal to 1 second. An [[InvalidInput]] exception is
    *   raised for invalid input value.
    */
  def setMaxAge(id: Id, expectedState: StreamState, age: FiniteDuration): F[WriteResult] =
    MaxAge(age).liftTo[F] >>= (ms.setMaxAge(id, expectedState, _))

  /** Sets max count in [[scala.Int]] for a stream and returns [[sec.api.WriteResult]] with current positions of the
    * stream after a successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    * @param count
    *   the max count [[scala.Int]] value for data in the stream. Valid values are greater or equal to 1. An
    *   [[InvalidInput]] exception is raised for invalid input value.
    */
  def setMaxCount(id: Id, expectedState: StreamState, count: Int): F[WriteResult] =
    MaxCount(count).liftTo[F] >>= (ms.setMaxCount(id, expectedState, _))

  /** Sets cache control in [[scala.concurrent.duration.FiniteDuration]] for a stream and returns
    * [[sec.api.WriteResult]] with current positions of the stream after a successful operation. Failure to fulfill the
    * expected state is manifested by raising [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    * @param cacheControl
    *   the cache control [[scala.concurrent.duration.FiniteDuration]] value for data in the stream. Valid values are
    *   [[scala.concurrent.duration.FiniteDuration]] greater or equal to 1 second. An [[InvalidInput]] exception is
    *   raised for invalid input value.
    */
  def setCacheControl(id: Id, expectedState: StreamState, cacheControl: FiniteDuration): F[WriteResult] =
    CacheControl(cacheControl).liftTo[F] >>= (ms.setCacheControl(id, expectedState, _))

  /** Sets truncated before in [[scala.Long]] for a stream and returns [[sec.api.WriteResult]] with current positions of
    * the stream after a successful operation. Failure to fulfill the expected state is manifested by raising
    * [[sec.api.exceptions.WrongExpectedState]].
    *
    * @param id
    *   the id of the stream.
    * @param expectedState
    *   the state that the stream is expected to in. See [[StreamState]] for details.
    * @param truncatedBefore
    *   the truncated before [[scala.Long]] value for data in the stream. Valid values are [[scala.Long]] greater or
    *   equal to 0L. An [[InvalidInput]] exception is raised for invalid input value.
    */
  def setTruncateBefore(id: Id, expectedState: StreamState, truncateBefore: Long): F[WriteResult] =
    ms.setTruncateBefore(id, expectedState, StreamPosition(truncateBefore))

}
