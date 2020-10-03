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

import cats.Eq

//======================================================================================================================

sealed trait Direction
object Direction {

  case object Forwards  extends Direction
  case object Backwards extends Direction

  implicit final private[sec] class DirectionOps(val d: Direction) extends AnyVal {
    def fold[A](fw: => A, bw: => A): A = d match {
      case Forwards  => fw
      case Backwards => bw
    }

  }

  implicit val eqForDirection: Eq[Direction] = Eq.fromUniversalEquals[Direction]

}

//======================================================================================================================
