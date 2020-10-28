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

import cats.data.NonEmptyList
import cats.syntax.all._

import EventFilter._

//======================================================================================================================

/**
 * Used for server-side event stream filtering. There are two *kinds* of filters:
 *
 *   - [[EventFilter.ByStreamId]] when you wish to filter by stream identifier.
 *   - [[EventFilter.ByEventType]] when you wish to filter by event type.
 *
 * In combination with [[EventFilter.Kind]] you can choose between two types of filter expressions:
 *
 *   - [[EventFilter.PrefixFilter]] when you wish to filter for prefixes. An example of this is
 *     `PrefixFilter("user_stream")` for streams starting with the string value `"user_stream"`
 *      like `"user_stream-a"` and `"user_stream-b"`.
 *
 *   - [[EventFilter.RegexFilter]] when you wish to filter with a regular expression. An example of this is
 *     `RegexFilter("^[^$].*")` when you for do not wish to retrieve events starting with `$`.
 */
final case class EventFilter(
  kind: Kind,
  option: Either[NonEmptyList[PrefixFilter], RegexFilter]
)

object EventFilter {

  sealed trait Kind
  case object ByStreamId  extends Kind
  case object ByEventType extends Kind

  def streamIdPrefix(fst: String, rest: String*): EventFilter  = prefix(ByStreamId, fst, rest: _*)
  def streamIdRegex(filter: String): EventFilter               = regex(ByStreamId, filter)
  def eventTypePrefix(fst: String, rest: String*): EventFilter = prefix(ByEventType, fst, rest: _*)
  def eventTypeRegex(filter: String): EventFilter              = regex(ByEventType, filter)

  def prefix(kind: Kind, fst: String, rest: String*): EventFilter =
    EventFilter(kind, NonEmptyList(PrefixFilter(fst), rest.toList.map(PrefixFilter)).asLeft)

  def regex(kind: Kind, filter: String): EventFilter =
    EventFilter(kind, RegexFilter(filter).asRight)

  ///

  sealed trait Expression
  final case class PrefixFilter(value: String) extends Expression
  final case class RegexFilter(value: String)  extends Expression

  object RegexFilter {
    val excludeSystemEvents: RegexFilter = apply("^[^$].*".r)
    def apply(regex: Regex): RegexFilter = RegexFilter(regex.pattern.toString)
  }

}

//======================================================================================================================

sealed abstract case class SubscriptionFilterOptions(
  filter: EventFilter,
  maxSearchWindow: Option[Int],
  checkpointIntervalMultiplier: Int
)

object SubscriptionFilterOptions {

  /**
   * @param filter See [[EventFilter]].
   * @param maxSearchWindow Maximum number of events to read that do not match the filter.
   *                        Minimum valid value is 1 and if provided value is less then 1 is used.
   * @param checkpointIntervalMultiplier The checkpoint interval is multiplied by the max search window to
   *                                     determine the number of events after which to checkpoint.
   *                                     Minimum valid value is 1 and if provided value is less then 1 is used.
   */
  def apply(
    filter: EventFilter,
    maxSearchWindow: Option[Int] = 32.some,
    checkpointIntervalMultiplier: Int = 1
  ): SubscriptionFilterOptions =
    new SubscriptionFilterOptions(filter, maxSearchWindow.map(_ max 1), checkpointIntervalMultiplier max 1) {}

}

//======================================================================================================================
