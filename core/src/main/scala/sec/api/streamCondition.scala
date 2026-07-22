/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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

/** The expected condition of a stream in a consistency check, echoed back in an
  * [[sec.api.exceptions.AppendConsistencyViolation]]. Any is absent by design: it is expressed as
  * the absence of a check and rejected as an explicit expected value.
  */
enum ExpectedCondition:
  case NoStream, StreamExists, SoftDeleted, Tombstoned
  case AtPosition(position: StreamPosition.Exact)

  def render: String = this match
    case NoStream        => "NoStream"
    case StreamExists    => "StreamExists"
    case SoftDeleted     => "SoftDeleted"
    case Tombstoned      => "Tombstoned"
    case AtPosition(p)   => s"position ${p.value.render}"

/** The actual condition of a stream reported in an [[sec.api.exceptions.AppendConsistencyViolation]]. */
enum ActualCondition:
  case NotFound, SoftDeleted, Tombstoned
  case AtPosition(position: StreamPosition.Exact)

  def render: String = this match
    case NotFound      => "NotFound"
    case SoftDeleted   => "SoftDeleted"
    case Tombstoned    => "Tombstoned"
    case AtPosition(p) => s"position ${p.value.render}"
