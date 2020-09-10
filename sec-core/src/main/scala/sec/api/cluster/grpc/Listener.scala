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
package cluster
package grpc

import io.grpc.NameResolver.{Listener2, ResolutionResult}
import cats.effect.Sync

private[sec] trait Listener[F[_]] {
  def onResult(result: ResolutionResult): F[Unit]
}

private[sec] object Listener {
  def apply[F[_]](l2: Listener2)(implicit F: Sync[F]): Listener[F] =
    (rr: ResolutionResult) => F.delay(l2.onResult(rr))
}
