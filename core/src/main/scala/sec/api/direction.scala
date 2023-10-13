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

import cats.Eq

//======================================================================================================================

/** Direction used when reading from a stream with variants:
  *
  *   - [[Direction.Forwards]] when you wish to read forwards.
  *   - [[Direction.Backwards]] when you wish to read backwards. This is useful when reading the latest
  *     [[StreamPosition.Exact]] value of a particular stream.
  */
sealed trait Direction
object Direction:

  case object Forwards extends Direction
  case object Backwards extends Direction

  extension (d: Direction)
    private[sec] def fold[A](fw: => A, bw: => A): A =
      d match
        case Forwards  => fw
        case Backwards => bw

  given Eq[Direction] = Eq.fromUniversalEquals[Direction]

//======================================================================================================================
