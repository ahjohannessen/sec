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

import scala.util.matching.Regex
import cats.syntax.all._
import cats.data.NonEmptyList
import EventFilter._

//======================================================================================================================

final case class EventFilter(
  kind: Kind,
  maxSearchWindow: Option[Int],
  option: Either[NonEmptyList[PrefixFilter], RegexFilter]
)

object EventFilter {

  sealed trait Kind
  case object ByStreamId  extends Kind
  case object ByEventType extends Kind

  def prefix(kind: Kind, maxSearchWindow: Option[Int], fst: String, rest: String*): EventFilter =
    EventFilter(kind, maxSearchWindow, NonEmptyList(PrefixFilter(fst), rest.toList.map(PrefixFilter)).asLeft)

  def regex(kind: Kind, maxSearchWindow: Option[Int], filter: String): EventFilter =
    EventFilter(kind, maxSearchWindow, RegexFilter(filter).asRight)

  ///

  sealed trait Expression
  final case class PrefixFilter(value: String) extends Expression
  final case class RegexFilter(value: String)  extends Expression

  object RegexFilter {
    val excludeSystemEvents: Regex       = "^[^$].*".r
    def apply(regex: Regex): RegexFilter = RegexFilter(regex.pattern.toString)
  }

}

//======================================================================================================================
