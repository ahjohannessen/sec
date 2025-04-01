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

import scala.concurrent.duration._
import cats.Endo
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import cats.effect.IO
import fs2._
import sec.syntax.all._
import helpers.implicits._

class SubscribeToAllFilterSuite extends SnSuite {

  import EventFilter._
  import StreamState.NoStream

  val maxSearchWindow = 32
  val multiplier      = 3

  val replaceType: String => Endo[EventData] =
    et => ed => EventData(et, ed.eventId, ed.data, ed.metadata, ed.contentType).unsafeGet

  val mkPrefix: String => String =
    p => s"streams_subscribe_to_all_filter_${p}_$genIdentifier"

  def writeRandom(amount: Int) = genStreamUuid[IO] >>= { sid =>
    streams.appendToStream(sid, NoStream, genEvents(amount))
  }

  //

  group("subscribing to non-existing streams") {
    runBeginning(includeBefore = false)
    runEnd(includeBefore       = false)
    runPosition(includeBefore  = false)
  }

  group("subscribing to existing streams") {
    runBeginning(includeBefore = true)
    runEnd(includeBefore       = true)
    runPosition(includeBefore  = true)
  }

  //

  def runBeginning(includeBefore: Boolean): Unit = {

    val existing = includeBefore.fold("exists", "non_present")
    val append   = s"_beginning_$existing"

    val siPrefix = mkPrefix(s"stream_id_prefix$append")
    val siRegex  = mkPrefix(s"stream_id_regex$append")
    val etPrefix = mkPrefix(s"event_type_prefix$append")
    val etRegex  = mkPrefix(s"event_type_regex_$append")

    val run = testBeginningOrEnd(None, includeBefore)

    test("from beginning: stream id prefix")(run(siPrefix, streamIdPrefix(siPrefix), identity))
    test("from beginning: stream id regex")(run(siRegex, streamIdRegex(siRegex), identity))
    test("from beginning: event type prefix")(run(etPrefix, eventTypePrefix(etPrefix), replaceType(etPrefix)))
    test("from beginning: event type regex")(run(etRegex, eventTypeRegex(etRegex), replaceType(etRegex)))

  }

  def runEnd(includeBefore: Boolean): Unit = {

    val existing = includeBefore.fold("exists", "non_present")
    val append   = s"_end_$existing"

    val siPrefix = mkPrefix(s"stream_id_prefix$append")
    val siRegex  = mkPrefix(s"stream_id_regex$append")
    val etPrefix = mkPrefix(s"event_type_prefix$append")
    val etRegex  = mkPrefix(s"event_type_regex_$append")

    val run = testBeginningOrEnd(LogPosition.End.some, includeBefore)

    test("from end: stream id prefix")(run(siPrefix, streamIdPrefix(siPrefix), identity))
    test("from end: stream id regex")(run(siRegex, streamIdRegex(siRegex), identity))
    test("from end: event type prefix")(run(etPrefix, eventTypePrefix(etPrefix), replaceType(etPrefix)))
    test("from end: event type regex")(run(etRegex, eventTypeRegex(etRegex), replaceType(etRegex)))

  }

  def runPosition(includeBefore: Boolean): Unit = {

    val existing = includeBefore.fold("exists", "non_present")
    val append   = s"_log_position_$existing"

    val siPrefix = mkPrefix(s"stream_id_prefix$append")
    val siRegex  = mkPrefix(s"stream_id_regex$append")
    val etPrefix = mkPrefix(s"event_type_prefix$append")
    val etRegex  = mkPrefix(s"event_type_regex_$append")

    val run = testPosition(includeBefore)

    test("from position: stream id prefix")(run(siPrefix, streamIdPrefix(siPrefix), identity))
    test("from position: stream id regex")(run(siRegex, streamIdRegex(siRegex), identity))
    test("from position: event type prefix")(run(etPrefix, eventTypePrefix(etPrefix), replaceType(etPrefix)))
    test("from position: event type regex")(run(etRegex, eventTypeRegex(etRegex), replaceType(etRegex)))

  }

  //

  def testBeginningOrEnd(from: Option[LogPosition.End.type], includeBefore: Boolean)(
    prefix: String,
    filter: EventFilter,
    adjustFn: Endo[EventData]
  ): IO[Unit] = {

    val options  = SubscriptionFilterOptions(filter, maxSearchWindow.some, multiplier)
    val before   = genEvents(10).map(adjustFn)
    val after    = genEvents(10).map(adjustFn)
    val expected = from.fold(includeBefore.fold(before.concatNel(after), after))(_ => after)

    def mkStreamId = genStreamId(s"${prefix}_")

    def write(eds: Nel[EventData]) =
      eds.traverse(e => streams.appendToStream(mkStreamId, NoStream, Nel.one(e)))

    val subscribe = streams.subscribeToAll(from, options).takeThrough(_.fold(_ => true, _.eventData != after.last))

    val writeBefore = write(before).whenA(includeBefore).void
    val writeAfter  = write(after).void

    val result = for {
      _    <- Stream.eval(writeRandom(100))
      _    <- Stream.eval(writeBefore)
      _    <- Stream.eval(writeRandom(100))
      _    <- Stream.sleep[IO](1.second)
      data <- subscribe.concurrently(Stream.eval(writeRandom(100) *> writeAfter).delayBy(1.second))
    } yield data

    result.compile.toList.map { r =>
      val (c, e) = r.partitionMap(identity)
      assertEquals(e.size, expected.size)
      assertEquals(e.map(_.eventData).toNel, Some(expected))
      assert(c.filterNot(_.isEndOfStream).nonEmpty)
    }
  }

  def testPosition(includeBefore: Boolean)(
    prefix: String,
    filter: EventFilter,
    adjustFn: Endo[EventData]
  ): IO[Unit] = {

    val options  = SubscriptionFilterOptions(filter, maxSearchWindow.some, multiplier)
    val before   = genEvents(10).map(adjustFn)
    val after    = genEvents(10).map(adjustFn)
    val expected = includeBefore.fold(before.concatNel(after), after)
    val existing = includeBefore.fold("existing_stream", "non_existing_stream")

    def mkStreamId =
      genStreamId(s"${prefix}_${existing}_from_log_position")

    def write(eds: Nel[EventData]) =
      eds.traverse(e => streams.appendToStream(mkStreamId, NoStream, Nel.one(e)))

    def subscribe(from: LogPosition) =
      streams.subscribeToAll(from.some, options).takeThrough(_.fold(_ => true, _.eventData != after.last))

    val writeBefore = write(before).whenA(includeBefore).void
    val writeAfter  = write(after).void

    val result = for {
      pos  <- Stream.eval(writeRandom(100)).map(_.logPosition)
      _    <- Stream.eval(writeBefore)
      _    <- Stream.eval(writeRandom(100))
      _    <- Stream.sleep[IO](1.second)
      data <- subscribe(pos).concurrently(Stream.eval(writeAfter).delayBy(1.second))
    } yield data

    result.compile.toList.map { r =>
      val (c, e) = r.partitionMap(identity)
      assertEquals(e.size, expected.size)
      assertEquals(e.map(_.eventData).toNel, Some(expected))
      assert(c.nonEmpty)
    }
  }

}
