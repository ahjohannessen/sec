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

import java.util.UUID
import java.{util => ju}
import scala.concurrent.duration._
import scodec.bits.ByteVector
import io.circe.Json
import cats.Endo
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import cats.effect.IO
import cats.effect.concurrent.Ref
import fs2._
import sec.api.Direction._
import sec.api.exceptions._
import sec.syntax.all._
import helpers.text.mkSnakeCase
import helpers.implicits._

class StreamsSuite extends SnSpec {

  sequential

  "Streams" should {

    //==================================================================================================================

    "subscribeToAll" >> {

      val streamPrefix                       = s"streams_subscribe_to_all_${genIdentifier}_"
      val fromBeginning: Option[LogPosition] = Option.empty
      val fromEnd: Option[LogPosition]       = LogPosition.End.some

      "works when streams do not exist prior to subscribing" >> {

        def mkId(suffix: String): StreamId =
          genStreamId(s"${streamPrefix}non_existing_stream_${suffix}_")

        def write(id: StreamId, data: Nel[EventData]) =
          Stream.eval(streams.appendToStream(id, StreamState.NoStream, data))

        def subscribe(exclusiveFrom: Option[LogPosition], filter: StreamId => Boolean) = streams
          .subscribeToAll(exclusiveFrom)
          .filter(e => e.streamId.isNormal && filter(e.streamId))

        def test(exclusiveFrom: Option[LogPosition]) = {

          val s1       = mkId("s1")
          val s2       = mkId("s2")
          val s1Events = genEvents(10)
          val s2Events = genEvents(10)
          val count    = (s1Events.size + s2Events.size).toLong

          val writeBoth = (write(s1, s1Events) ++ write(s2, s2Events)).delayBy(1.second)
          val run       = subscribe(exclusiveFrom, Set(s1, s2).contains).concurrently(writeBoth)

          run.take(count).compile.toList.map { events =>
            events.size.toLong shouldEqual count
            events.filter(_.streamId == s1).map(_.eventData).toNel should beSome(s1Events)
            events.filter(_.streamId == s2).map(_.eventData).toNel should beSome(s2Events)

          }

        }

        "from beginning" >> {
          test(fromBeginning)
        }

        "from log position" >> {

          val initId  = mkId("init")
          val prepare = write(initId, genEvents(10)).map(_.logPosition)
          prepare.compile.lastOrError >>= { pos => test(pos.some) }
        }

        "from end" >> {
          test(fromEnd)
        }

      }

      "works when streams exist prior to subscribing" >> {

        def mkId(suffix: String): StreamId =
          genStreamId(s"${streamPrefix}existing_stream_${suffix}_")

        def write(id: StreamId, data: Nel[EventData], st: StreamState = StreamState.NoStream) =
          Stream.eval(streams.appendToStream(id, st, data))

        def subscribe(exclusiveFrom: Option[LogPosition], filter: StreamId => Boolean) =
          streams.subscribeToAll(exclusiveFrom).filter(e => e.streamId.isNormal && filter(e.streamId))

        val s1Before = genEvents(12)
        val s2Before = genEvents(8)

        val s1After = genEvents(3)
        val s2After = genEvents(7)

        val before = s1Before.concatNel(s2Before)
        val after  = s1After.concatNel(s2After)
        val all    = before.concatNel(after)

        def writeBefore(s1: StreamId, s2: StreamId) =
          write(s1, s1Before) >>= { wa => write(s2, s2Before).map(wb => (wa, wb)) }

        def writeAfter(s1: StreamId, r1: StreamState, s2: StreamId, r2: StreamState) =
          (write(s1, s1After, r1) >> write(s2, s2After, r2)).delayBy(500.millis)

        def test(exclusiveFrom: Option[LogPosition], s1: StreamId, s2: StreamId) =
          writeBefore(s1, s2) >>= { case (wa, wb) =>
            subscribe(exclusiveFrom, Set(s1, s2).contains)
              .concurrently(writeAfter(s1, wa.streamPosition, s2, wb.streamPosition))
              .map(_.eventData)
          }

        "from beginning" >> {

          val s1    = mkId("s1_begin")
          val s2    = mkId("s2_begin")
          val count = all.size.toLong

          test(fromBeginning, s1, s2).take(count).compile.toList.map(_.toNel should beSome(all))
        }

        "from log position" >> {

          val s1       = mkId("s1_pos")
          val s2       = mkId("s2_pos")
          val expected = s2Before.concatNel(after)
          val count    = expected.size.toLong

          val result = writeBefore(s1, s2) >>= { case (wrs1, wrs2) =>
            subscribe(wrs1.logPosition.some, Set(s1, s2).contains)
              .concurrently(writeAfter(s1, wrs1.streamPosition, s2, wrs2.streamPosition))
              .map(_.eventData)
              .take(count)
          }

          result.compile.toList.map(_.toNel should beSome(expected))
        }

        "from end" >> {

          val s1    = mkId("s1_end")
          val s2    = mkId("s2_end")
          val count = after.size.toLong

          test(fromEnd, s1, s2).take(count).compile.toList.map(_.toNel should beSome(after))
        }

      }

    }

    //==================================================================================================================

    "subscribeToAll with filter" >> {

      import EventFilter._
      import StreamState.NoStream

      val maxSearchWindow = 32
      val multiplier      = 3

      def writeRandom(amount: Int) = genStreamUuid[IO] >>= { sid =>
        streams.appendToStream(sid, NoStream, genEvents(amount))
      }

      ///

      def testBeginningOrEnd(from: Option[LogPosition.End.type], includeBefore: Boolean)(
        prefix: String,
        filter: EventFilter,
        adjustFn: Endo[EventData]
      ) = {

        val options  = SubscriptionFilterOptions(filter, maxSearchWindow.some, multiplier)
        val before   = genEvents(10).map(adjustFn)
        val after    = genEvents(10).map(adjustFn)
        val expected = from.fold(includeBefore.fold(before.concatNel(after), after))(_ => after)

        def mkStreamId = genStreamId(s"${prefix}_")

        def write(eds: Nel[EventData]) =
          eds.traverse(e => streams.appendToStream(mkStreamId, NoStream, Nel.one(e)))

        val subscribe =
          streams.subscribeToAllFiltered(from, options).takeThrough(_.fold(_ => true, _.eventData != after.last))

        val writeBefore = write(before).whenA(includeBefore).void
        val writeAfter  = write(after).void

        val result = for {
          _    <- Stream.eval(writeRandom(100))
          _    <- Stream.eval(writeBefore)
          _    <- Stream.eval(writeRandom(100))
          _    <- Stream.sleep(500.millis)
          data <- subscribe.concurrently(Stream.eval(writeAfter).delayBy(500.millis))
        } yield data

        result.compile.toList.map { r =>
          val (c, e) = r.partitionMap(identity)
          e.size shouldEqual expected.size
          e.map(_.eventData).toNel should beSome(expected)
          c.nonEmpty should beTrue
        }
      }

      ///

      def testPosition(includeBefore: Boolean)(
        prefix: String,
        filter: EventFilter,
        adjustFn: Endo[EventData]
      ) = {

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
          streams.subscribeToAllFiltered(from.some, options).takeThrough(_.fold(_ => true, _.eventData != after.last))

        val writeBefore = write(before).whenA(includeBefore).void
        val writeAfter  = write(after).void

        val result = for {
          pos  <- Stream.eval(writeRandom(100)).map(_.logPosition)
          _    <- Stream.eval(writeBefore)
          _    <- Stream.eval(writeRandom(100))
          _    <- Stream.sleep(500.millis)
          data <- subscribe(pos).concurrently(Stream.eval(writeAfter).delayBy(500.millis))
        } yield data

        result.compile.toList.map { r =>
          val (c, e) = r.partitionMap(identity)
          e.size shouldEqual expected.size
          e.map(_.eventData).toNel should beSome(expected)
          c.nonEmpty should beTrue
        }
      }

      ///

      val replaceType: String => Endo[EventData] =
        et => ed => EventData(et, ed.eventId, ed.data, ed.metadata, ed.contentType).unsafe

      val mkPrefix: String => String =
        p => s"streams_subscribe_to_all_filter_${p}_$genIdentifier"

      def runBeginningAndEnd(includeBefore: Boolean) = {

        val existing = includeBefore.fold("exists", "non_present")

        "from beginning" >> {

          val append = s"_beginning_$existing"

          val siPrefix = mkPrefix(s"stream_id_prefix$append")
          val siRegex  = mkPrefix(s"stream_id_regex$append")
          val etPrefix = mkPrefix(s"event_type_prefix$append")
          val etRegex  = mkPrefix(s"event_type_regex_$append")

          val testBeginning = testBeginningOrEnd(None, includeBefore) _

          "stream id prefix" >> testBeginning(siPrefix, streamIdPrefix(siPrefix), identity)
          "stream id regex" >> testBeginning(siRegex, streamIdRegex(siRegex), identity)
          "event type prefix" >> testBeginning(etPrefix, eventTypePrefix(etPrefix), replaceType(etPrefix))
          "event type regex" >> testBeginning(etRegex, eventTypeRegex(etRegex), replaceType(etRegex))

        }

        "from end" >> {

          val append   = s"_end_$existing"
          val siPrefix = mkPrefix(s"stream_id_prefix$append")
          val siRegex  = mkPrefix(s"stream_id_regex$append")
          val etPrefix = mkPrefix(s"event_type_prefix$append")
          val etRegex  = mkPrefix(s"event_type_regex_$append")

          val testEnd = testBeginningOrEnd(LogPosition.End.some, includeBefore) _

          "stream id prefix" >> testEnd(siPrefix, streamIdPrefix(siPrefix), identity)
          "stream id regex" >> testEnd(siRegex, streamIdRegex(siRegex), identity)
          "event type prefix" >> testEnd(etPrefix, eventTypePrefix(etPrefix), replaceType(etPrefix))
          "event type regex" >> testEnd(etRegex, eventTypeRegex(etRegex), replaceType(etRegex))

        }

      }

      def runPosition(includeBefore: Boolean) = {

        val existing = includeBefore.fold("exists", "non_present")

        "from log position" >> {

          val append   = s"_log_position_$existing"
          val siPrefix = mkPrefix(s"stream_id_prefix$append")
          val siRegex  = mkPrefix(s"stream_id_regex$append")
          val etPrefix = mkPrefix(s"event_type_prefix$append")
          val etRegex  = mkPrefix(s"event_type_regex_$append")

          val test = testPosition(includeBefore) _

          "stream id prefix" >> test(siPrefix, streamIdPrefix(siPrefix), identity)
          "stream id regex" >> test(siRegex, streamIdRegex(siRegex), identity)
          "event type prefix" >> test(etPrefix, eventTypePrefix(etPrefix), replaceType(etPrefix))
          "event type regex" >> test(etRegex, eventTypeRegex(etRegex), replaceType(etRegex))

        }
      }

      "works when streams does exist not prior to subscribing" >> {
        runBeginningAndEnd(includeBefore = false)
        runPosition(includeBefore        = false)
      }

      "works when streams exist prior to subscribing" >> {
        runBeginningAndEnd(includeBefore = true)
        runPosition(includeBefore        = true)
      }

    }

    //==================================================================================================================

    "subscribeToStream" >> {

      val streamPrefix                                       = s"streams_subscribe_to_stream_${genIdentifier}_"
      val fromBeginning: Option[StreamPosition]              = Option.empty
      val fromStreamPosition: Long => Option[StreamPosition] = r => StreamPosition.exact(r).some
      val fromEnd: Option[StreamPosition]                    = StreamPosition.End.some

      "works when stream does not exist prior to subscribing" >> {

        val events = genEvents(50)

        def test(exclusivefrom: Option[StreamPosition], takeCount: Int) = {

          val id        = genStreamId(s"${streamPrefix}non_existing_stream_")
          val subscribe = streams.subscribeToStream(id, exclusivefrom).take(takeCount.toLong).map(_.eventData)
          val write     = Stream.eval(streams.appendToStream(id, StreamState.NoStream, events)).delayBy(500.millis)
          val result    = subscribe.concurrently(write)

          result.compile.toList
        }

        "from beginning" >> {
          test(fromBeginning, events.size).map(_ shouldEqual events.toList)
        }

        "from stream position" >> {
          test(fromStreamPosition(4), events.size - 5).map(_ shouldEqual events.toList.drop(5))
        }

        "from end" >> {
          test(fromEnd, events.size).map(_ shouldEqual events.toList)
        }

      }

      "works with multiple subscriptions to same stream" >> {

        val eventCount      = 10
        val subscriberCount = 4
        val events          = genEvents(eventCount)

        def test(exclusivFrom: Option[StreamPosition], takeCount: Int) = {

          val id    = genStreamId(s"${streamPrefix}multiple_subscriptions_to_same_stream_")
          val write = Stream.eval(streams.appendToStream(id, StreamState.NoStream, events)).delayBy(500.millis)

          def mkSubscribers(onEvent: IO[Unit]): Stream[IO, Event] = Stream
            .emit(streams.subscribeToStream(id, exclusivFrom).evalTap(_ => onEvent).take(takeCount.toLong))
            .repeat
            .take(subscriberCount.toLong)
            .parJoin(subscriberCount)

          val result: Stream[IO, Int] = Stream.eval(Ref.of[IO, Int](0)) >>= { ref =>
            Stream(mkSubscribers(ref.update(_ + 1)), write).parJoinUnbounded >> Stream.eval(ref.get)
          }

          result.compile.lastOrError.map(_.shouldEqual(takeCount * subscriberCount))
        }

        "from beginning" >> {
          test(fromBeginning, eventCount)
        }

        "from stream position" >> {
          test(fromStreamPosition(0), eventCount - 1)
        }

        "from end" >> {
          test(fromEnd, eventCount)
        }

      }

      "works with existing stream" >> {

        val beforeEvents = genEvents(40)
        val afterEvents  = genEvents(10)
        val totalEvents  = beforeEvents.concatNel(afterEvents)

        def test(exclusiveFrom: Option[StreamPosition], takeCount: Int) = {

          val id = genStreamId(s"${streamPrefix}existing_and_new_")

          val beforeWrite =
            Stream.eval(streams.appendToStream(id, StreamState.NoStream, beforeEvents))

          def afterWrite(st: StreamState): Stream[IO, WriteResult] =
            Stream.eval(streams.appendToStream(id, st, afterEvents)).delayBy(500.millis)

          def subscribe(onEvent: Event => IO[Unit]): Stream[IO, Event] =
            streams.subscribeToStream(id, exclusiveFrom).evalTap(onEvent).take(takeCount.toLong)

          val result: Stream[IO, List[EventData]] = for {
            ref        <- Stream.eval(Ref.of[IO, List[EventData]](Nil))
            st         <- beforeWrite.map(_.streamPosition)
            _          <- Stream.sleep(500.millis)
            _          <- subscribe(e => ref.update(_ :+ e.eventData)).concurrently(afterWrite(st))
            readEvents <- Stream.eval(ref.get)
          } yield readEvents

          result.compile.lastOrError

        }

        "from beginning - reads all events and listens for new ones" >> {
          test(fromBeginning, totalEvents.size).map(_.toNel shouldEqual totalEvents.some)
        }

        "from stream position - reads events after stream position and listens for new ones" >> {
          test(fromStreamPosition(29), 20).map(_ shouldEqual totalEvents.toList.drop(30))
        }

        "from end - listens for new events at given end of stream" >> {
          test(fromEnd, afterEvents.size).map(_.toNel shouldEqual afterEvents.some)
        }

      }

      "raises when stream is tombstoned" >> {

        def test(exclusiveFrom: Option[StreamPosition]) = {

          val id        = genStreamId(s"${streamPrefix}stream_is_tombstoned_")
          val subscribe = streams.subscribeToStream(id, exclusiveFrom)
          val delete    = Stream.eval(streams.tombstone(id, StreamState.Any)).delayBy(500.millis)
          val expected  = StreamDeleted(id.stringValue).asLeft

          subscribe.concurrently(delete).compile.last.attempt.map(_.shouldEqual(expected))
        }

        "from beginning" >> {
          test(fromBeginning)
        }

        "from stream position" >> {
          test(fromStreamPosition(5))
        }

        "from end" >> {
          test(fromEnd)
        }

      }

    }

    //==================================================================================================================

    "readAll" >> {

      import LogPosition._

      val streamPrefix    = s"streams_read_all_${genIdentifier}_"
      val eventTypePrefix = s"sec.$genIdentifier.Event"

      val id1     = genStreamId(streamPrefix)
      val id2     = genStreamId(streamPrefix)
      val events1 = genEvents(500, eventTypePrefix)
      val events2 = genEvents(500, eventTypePrefix)
      val written = events1 ::: events2

      val writeEvents =
        streams.appendToStream(id1, StreamState.NoStream, events1) *>
          streams.appendToStream(id2, StreamState.NoStream, events2)

      def read(from: LogPosition, direction: Direction, maxCount: Long = 1) =
        streams.readAll(from, direction, maxCount, resolveLinkTos = false).compile.toList

      //

      "init" >> writeEvents.as(ok)

      //

      "forwards" >> {

        "reading from start yields events" >> {
          read(Start, Forwards, written.size.toLong).map(_.size shouldEqual written.size)
        }

        "reading from end yields no events" >> {
          read(End, Forwards, 10).map(_.isEmpty should beTrue)
        }

        "events are in same order as written" >> {
          read(Start, Forwards, Int.MaxValue - 1)
            .map(_.collect { case e if e.streamId == id1 => e.eventData })
            .map(_ shouldEqual events1.toList)
        }

        "max count <= 0 yields no events" >> {
          read(Start, Forwards, 0).map(_.isEmpty should beTrue)
        }

        "max count is respected" >> {
          streams
            .readAll(Start, Forwards, events1.length / 2L, resolveLinkTos = false)
            .take(events1.length.toLong)
            .compile
            .toList
            .map(_.size shouldEqual events1.length / 2)
        }

        def deleted(normalDelete: Boolean) = {

          val suffix = if (normalDelete) "" else "tombstoned"
          val id     = genStreamId(s"streams_read_all_deleted_${suffix}_")
          val events = genEvents(10)
          val write  = streams.appendToStream(id, StreamState.NoStream, events)

          def delete(er: StreamPosition.Exact) =
            if (normalDelete) streams.delete(id, er) else streams.tombstone(id, er)

          val decodeJson: EventRecord => IO[StreamMetadata] =
            _.eventData.data.decodeUtf8.liftTo[IO] >>= {
              io.circe.parser.decode[StreamMetadata](_).liftTo[IO]
            }

          val verifyDeleted =
            streams.readStreamForwards(id, maxCount = 1).compile.drain.recoverWith {
              case e: StreamNotFound if normalDelete && e.streamId.eqv(id.stringValue) => IO.unit
              case e: StreamDeleted if !normalDelete && e.streamId.eqv(id.stringValue) => IO.unit
            }

          val read = streams
            .readAllForwards()
            .filter(e => e.streamId.eqv(id) || e.streamId.eqv(id.metaId))
            .compile
            .toList

          val setup = write >>= { wr => delete(wr.streamPosition).void >> verifyDeleted >> read }

          def verify(es: List[Event]) =
            es.lastOption.toRight(new RuntimeException("expected metadata")).liftTo[IO] >>= { ts =>
              if (normalDelete) {
                es.dropRight(1).map(_.eventData) shouldEqual events.toList
                ts.streamId shouldEqual id.metaId
                ts.eventData.eventType shouldEqual EventType.StreamMetadata
                decodeJson(ts.record).map(_.state.truncateBefore.map(_.value) should beSome(Long.MaxValue))
              } else {
                es.dropRight(1).map(_.eventData) shouldEqual events.toList
                ts.streamId shouldEqual id
                ts.eventData.eventType shouldEqual EventType.StreamDeleted
                ok.pure[IO]
              }
            }

          setup >>= verify

        }

        "deleted stream" >> deleted(normalDelete = true)
        "tombstoned stream" >> deleted(normalDelete = false)

        "max count deleted events are not resolved" >> {

          val deletedId               = genStreamId("streams_read_all_linkto_deleted_")
          val linkId                  = genStreamId("streams_read_all_linkto_link_")
          def encode(content: String) = ByteVector.encodeUtf8(content).unsafe

          def linkData(number: Long) =
            Nel.one(
              EventData(
                EventType.LinkTo,
                ju.UUID.randomUUID(),
                encode(s"$number@${deletedId.stringValue}"),
                ByteVector.empty,
                ContentType.Binary
              ))

          def append(id: StreamId, data: Nel[EventData]) =
            streams.appendToStream(id, StreamState.Any, data)

          def readLink(resolve: Boolean) =
            streams.readStreamForwards(linkId, maxCount = 3, resolveLinkTos = resolve).map(_.eventData).compile.toList

          val e1 = genEvents(1)
          val e2 = genEvents(1)
          val e3 = genEvents(1)

          val l1 = linkData(0)
          val l2 = linkData(1)
          val l3 = linkData(2)

          append(deletedId, e1) >>
            metaStreams.setMaxCount(deletedId, StreamState.NoStream, 2) >>
            append(deletedId, e2) >>
            append(deletedId, e3) >>
            append(linkId, l1) >>
            append(linkId, l2) >>
            append(linkId, l3) >>
            readLink(resolve = true).map(_ shouldEqual (e2 ::: e3).toList) >>
            readLink(resolve = false).map(_ shouldEqual (l1 ::: l2 ::: l3).toList)
        }

      }

      "backwards" >> {

        "reading from start yields no events" >> {
          read(Start, Backwards).map(_.isEmpty should beTrue)
        }

        "reading from end yields events" >> {
          read(End, Backwards).map(_.lastOption should beSome)
        }

        "events are in reverse order as written" >> {
          read(End, Backwards, Int.MaxValue - 1)
            .map(_.collect { case e if e.streamId == id1 => e.eventData })
            .map(_.reverse shouldEqual events1.toList)
        }

        "max count <= 0 yields no events" >> {
          read(End, Backwards, 0).map(_.isEmpty should beTrue)
        }

        "max count is respected" >> {
          streams
            .readAll(End, Backwards, events1.length / 2L, resolveLinkTos = false)
            .take(events1.length.toLong)
            .compile
            .toList
            .map(_.size shouldEqual events1.length / 2)
        }

      }

    }

    //==================================================================================================================

    "readStream" >> {

      import StreamPosition._

      val streamPrefix = s"streams_read_stream_${genIdentifier}_"
      val id           = genStreamId(streamPrefix)
      val events       = genEvents(25)
      val writeEvents  = streams.appendToStream(id, StreamState.NoStream, events)

      def read(id: StreamId, from: StreamPosition, direction: Direction, count: Long = 50) =
        streams.readStream(id, from, direction, count, resolveLinkTos = false).compile.toList

      def readData(id: StreamId, from: StreamPosition, direction: Direction, count: Long = 50) =
        read(id, from, direction, count).map(_.map(_.eventData))

      //

      "init" >> writeEvents.as(ok)

      //

      "forwards" >> {

        "reading from start yields events" >> {
          read(id, Start, Forwards).map(_.size shouldEqual events.size)
        }

        "reading from start with count <= 0 yields no events" >> {
          read(id, Start, Forwards, -1L).map(_.isEmpty should beTrue)
          read(id, Start, Forwards, 0).map(_.isEmpty should beTrue)
        }

        "reading from end yields no events" >> {
          read(id, End, Forwards).map(_.isEmpty should beTrue)
        }

        "events are in same order as written" >> {
          readData(id, Start, Forwards).map(_ shouldEqual events.toList)
        }

        "reading non-existing stream raises stream not found" >> {

          val id = genStreamId(s"${streamPrefix}non_existing_forwards_")
          val ex = StreamNotFound(id.stringValue)

          read(id, Start, Forwards, 1).attempt.map(_ should beLeft(ex))
        }

        "reading deleted stream raises stream not found" >> {

          val id = genStreamId(s"${streamPrefix}stream_deleted_forwards_")
          val ex = StreamNotFound(id.stringValue)

          streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
            streams.delete(id, StreamState.Any) *>
            read(id, Start, Forwards, 5).attempt.map(_ should beLeft(ex))
        }

        "reading tombstoned stream raises stream deleted" >> {

          val id = genStreamId(s"${streamPrefix}stream_tombstoned_forwards_")
          val ex = StreamDeleted(id.stringValue)

          streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
            streams.tombstone(id, StreamState.Any) *>
            read(id, Start, Forwards, 5).attempt.map(_ should beLeft(ex))
        }

        "reading single event from arbitrary stream position" >> {
          readData(id, exact(7), Forwards, 1).map(_.lastOption shouldEqual events.get(7))
        }

        "reading from arbitrary stream position" >> {
          readData(id, exact(3L), Forwards, 2).map(_ shouldEqual events.toList.slice(3, 5))
        }

        "max count is respected" >> {
          streams
            .readStream(id, Start, Forwards, events.length / 2L, resolveLinkTos = false)
            .take(events.length.toLong)
            .compile
            .toList
            .map(_.size shouldEqual events.length / 2)
        }

      }

      "backwards" >> {

        "reading one from start yields first event" >> {
          readData(id, Start, Backwards, 1).map { e =>
            e.lastOption should beSome(events.head)
            e.size shouldEqual 1
          }
        }

        "reading one from end yields last event" >> {
          readData(id, End, Backwards, 1).map { es =>
            es.headOption should beSome(events.last)
            es.size shouldEqual 1
          }
        }

        "reading from end with max count <= 0 yields no events" >> {
          read(id, End, Backwards, -1).map(_.isEmpty should beTrue)
          read(id, End, Backwards, 0).map(_.isEmpty should beTrue)
        }

        "events are in reverse order as written" >> {
          readData(id, End, Backwards).map(_.reverse shouldEqual events.toList)
        }

        "reading non-existing stream raises stream not found" >> {
          val id = genStreamId(s"${streamPrefix}non_existing_backwards_")
          val ex = StreamNotFound(id.stringValue)
          read(id, End, Backwards, 1).attempt.map(_ should beLeft(ex))
        }

        "reading deleted stream raises stream not found" >> {

          val id = genStreamId(s"${streamPrefix}stream_deleted_backwards_")
          val ex = StreamNotFound(id.stringValue)

          streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
            streams.delete(id, StreamState.Any) *>
            read(id, End, Backwards, 5).attempt.map(_ should beLeft(ex))
        }

        "reading tombstoned stream raises stream deleted" >> {

          val id = genStreamId(s"${streamPrefix}stream_tombstoned_backwards_")
          val ex = StreamDeleted(id.stringValue)

          streams.appendToStream(id, StreamState.NoStream, genEvents(5)) *>
            streams.tombstone(id, StreamState.Any) *>
            read(id, End, Backwards, 5).attempt.map(_ should beLeft(ex))
        }

        "reading single event from arbitrary stream position" >> {
          readData(id, StreamPosition.exact(20), Backwards, 1).map(_.lastOption shouldEqual events.get(20))
        }

        "reading from arbitrary stream position" >> {
          readData(id, StreamPosition.exact(3L), Backwards, 2).map(_ shouldEqual events.toList.slice(2, 4).reverse)
        }

        "max count is respected" >> {
          streams
            .readStream(id, End, Backwards, events.length / 2L, resolveLinkTos = false)
            .take(events.length.toLong)
            .compile
            .toList
            .map(_.size shouldEqual events.length / 2)
        }

      }

    }

    //==================================================================================================================

    "appendToStream" >> {

      val streamPrefix = s"streams_append_to_stream_${genIdentifier}_"

      "create stream on first write if does not exist" >> {

        val events = genEvents(1)

        def test(expectedState: StreamState) = {

          val id = genStreamId(s"${streamPrefix}non_existing_${mkSnakeCase(expectedState.show)}_")

          streams.appendToStream(id, expectedState, events) >>= { wr =>
            streams.readStreamForwards(id, maxCount = 2).compile.toList.map { el =>
              el.map(_.eventData).toNel shouldEqual events.some
              wr.streamPosition shouldEqual StreamPosition.Start
            }
          }
        }

        "works with any expected stream state" >> {
          test(StreamState.Any)
        }

        "works with no stream expected stream state" >> {
          test(StreamState.NoStream)
        }

        "raises with exact expected stream state" >> {
          test(StreamPosition.Start).attempt.map {
            _ should beLike { case Left(WrongExpectedState(_, StreamPosition.Start, StreamState.NoStream)) => ok }
          }
        }

        "raises with stream exists expected stream state" >> {
          test(StreamState.StreamExists).attempt.map {
            _ should beLike { case Left(WrongExpectedState(_, StreamState.StreamExists, StreamState.NoStream)) =>
              ok
            }
          }
        }

      }

      "multiple idempotent writes" >> {

        "with unique uuids" >> {

          val id     = genStreamId(s"${streamPrefix}multiple_idempotent_writes_")
          val events = genEvents(4)
          val write  = streams.appendToStream(id, StreamState.Any, events)

          write >>= { first =>
            write.map { second =>
              first.streamPosition shouldEqual second.streamPosition
              first.streamPosition shouldEqual StreamPosition.exact(3L)
            }
          }

        }

        "with same uuids (bug in ESDB)" >> {

          val id     = genStreamId(s"${streamPrefix}multiple_idempotent_writes_same_uuid_")
          val event  = genEvents(1).head
          val events = Nel.of(event, List.fill(5)(event): _*)
          val write  = streams.appendToStream(id, StreamState.Any, events)

          write.map(_.streamPosition shouldEqual StreamPosition.exact(5))
        }

      }

      "multiple writes of multiple events with same uuids using expected stream state" >> {

        def test(expectedState: StreamState, secondExpectedState: StreamState) = {

          val st     = mkSnakeCase(expectedState.show)
          val id     = genStreamId(s"${streamPrefix}multiple_writes_multiple_events_same_uuid_${st}_")
          val event  = genEvents(1).head
          val events = Nel.of(event, List.fill(5)(event): _*)
          val write  = streams.appendToStream(id, expectedState, events)

          write >>= { first =>
            write.map { second =>
              first.streamPosition shouldEqual StreamPosition.exact(5)
              second.streamPosition shouldEqual secondExpectedState
            }
          }

        }

        "any then next expected stream state is unreliable" >> {
          test(StreamState.Any, StreamPosition.Start)
        }

        "no stream then next expected stream state is correct" >> {
          test(StreamState.NoStream, StreamPosition.exact(5))
        }

      }

      "append to tombstoned stream raises" >> {

        def test(expectedState: StreamState) = {
          val st     = mkSnakeCase(expectedState.show)
          val id     = genStreamId(s"${streamPrefix}tombstoned_stream_${st}_")
          val events = genEvents(1)
          val delete = streams.tombstone(id, StreamState.NoStream)
          val write  = streams.appendToStream(id, expectedState, events)

          delete >> write.attempt.map(_ shouldEqual StreamDeleted(id.stringValue).asLeft)
        }

        "with correct expected stream state" >> {
          test(StreamState.NoStream)
        }

        "with any expected stream state" >> {
          test(StreamState.Any)
        }

        "with stream exists expected stream state" >> {
          test(StreamState.StreamExists)
        }

        "with incorrect expected stream state" >> {
          test(StreamPosition.exact(5))
        }

      }

      "append to existing stream" >> {

        def test(sndExpectedState: StreamState) = {

          val st                      = mkSnakeCase(sndExpectedState.show)
          val id                      = genStreamId(s"${streamPrefix}existing_stream_with_${st}_")
          def write(esr: StreamState) = streams.appendToStream(id, esr, genEvents(1))

          write(StreamState.NoStream) >>= { first =>
            write(sndExpectedState).map { second =>
              first.streamPosition shouldEqual StreamPosition.Start
              second.streamPosition
            }
          }
        }

        "works with correct expected stream state" >> {
          test(StreamPosition.Start).map(_ shouldEqual StreamPosition.exact(1))
        }

        "works with any expected stream state" >> {
          test(StreamState.Any).map(_ shouldEqual StreamPosition.exact(1))
        }

        "works with stream exists expected stream state" >> {
          test(StreamState.StreamExists).map(_ shouldEqual StreamPosition.exact(1))
        }

        "raises with incorrect expected stream state" >> {
          val expected = StreamPosition.exact(1L)
          test(expected).attempt.map {
            _ should beLike { case Left(WrongExpectedState(_, `expected`, StreamPosition.Start)) => ok }
          }
        }

      }

      "append to stream with multiple events and stream exists expected stream state " >> {

        val id      = genStreamId(s"${streamPrefix}multiple_events_and_stream_exists_")
        val events  = genEvents(5)
        val writes  = events.toList.map(e => streams.appendToStream(id, StreamState.Any, Nel.one(e)))
        val prepare = Stream.eval(writes.sequence).compile.drain

        prepare >> streams
          .appendToStream(id, StreamState.StreamExists, genEvents(1))
          .map(_.streamPosition shouldEqual StreamPosition.exact(5))

      }

      "append to stream with stream exists expected version works if metadata stream exists" >> {

        val id    = genStreamId(s"${streamPrefix}stream_exists_and_metadata_stream_exists_")
        val meta  = metaStreams.setMaxAge(id, StreamState.NoStream, 10.seconds)
        val write = streams.appendToStream(id, StreamState.StreamExists, genEvents(1))

        meta >> write.map(_.streamPosition shouldEqual StreamPosition.Start)

      }

      "append to deleted stream" >> {

        def test(expectedState: StreamState) = {

          val st = mkSnakeCase(expectedState.show)
          val id = genStreamId(s"${streamPrefix}stream_exists_and_deleted_${st}_")

          streams.delete(id, StreamState.NoStream) >>
            streams.appendToStream(id, expectedState, genEvents(1))
        }

        "with stream exists expected version raises" >> {
          test(StreamState.StreamExists).attempt.map {
            _ should beLike { case Left(StreamDeleted(_)) => ok }
          }
        }

      }

      "can append multiple events at once" >> {

        val id       = genStreamId(s"${streamPrefix}multiple_events_at_once_")
        val events   = genEvents(100)
        val expected = StreamPosition.exact(99)

        streams.appendToStream(id, StreamState.NoStream, events).map(_.streamPosition shouldEqual expected)

      }

      "append events with size" >> {

        val max = 1024 * 1024 // Default ESDB setting

        def mkEvent(sizeBytes: Int): IO[EventData] = IO(UUID.randomUUID()).map { uuid =>

          val et       = EventType("et").unsafe
          val data     = ByteVector.fill(sizeBytes.toLong)(0)
          val metadata = ByteVector.empty
          val ct       = ContentType.Binary

          EventData(et, uuid, data, metadata, ct)
        }

        "less than or equal max append size works" >> {

          val id       = genStreamId(s"${streamPrefix}append_size_less_or_equal_bytes_")
          val equal    = List(mkEvent(max / 2), mkEvent(max / 2)).sequence.map(Nel.fromListUnsafe)
          val lessThan = List(mkEvent(max / 4), mkEvent(max / 2)).sequence.map(Nel.fromListUnsafe)

          def run(st: StreamState)(data: Nel[EventData]) =
            streams.appendToStream(id, st, data).as(ok)

          (equal >>= run(StreamState.NoStream)) >> (lessThan >>= run(StreamPosition.exact(1)))

        }

        "greater than max append size raises" >> {

          val id          = genStreamId(s"${streamPrefix}append_size_exceeds_bytes_")
          val greaterThan = List(mkEvent(max / 2), mkEvent(max / 2), mkEvent(max + 1)).sequence.map(Nel.fromListUnsafe)

          greaterThan >>= { events =>
            streams.appendToStream(id, StreamState.NoStream, events).attempt.map {
              _ shouldEqual MaximumAppendSizeExceeded(max.some).asLeft
            }
          }

        }

      }

      "append to implicitly created streams" >> {

        /*
         * sequence - events written to stream
         * 0em1 - event number 0 written with expected state -1 (minus 1)
         * 1any - event number 1 written with expected state any
         * S_0em1_1em1_E - START bucket, two events in bucket, END bucket
         *
         *   See: https://github.com/EventStore/EventStore/blob/master/src/EventStore.Core.Tests/ClientAPI/appending_to_implicitly_created_stream.cs
         */

        def mkId = genStreamId(s"${streamPrefix}implicitly_created_stream_")

        "sequence 0em1 1e0 2e1 3e2 4e3 5e4" >> {

          def run(nextExpected: StreamState) = {

            val id     = mkId
            val events = genEvents(6)

            for {
              _ <- streams.appendToStream(id, StreamState.NoStream, events)
              _ <- streams.appendToStream(id, nextExpected, Nel.one(events.head))
              e <- streams.readStreamForwards(id, maxCount = events.size + 1L).compile.toList

            } yield e.size shouldEqual events.size
          }

          "0em1 is idempotent" >> {
            run(StreamState.NoStream)
          }

          "0any is idempotent" >> {
            run(StreamState.Any)
          }
        }

        "sequence 0em1 1e0 2e1 3e2 4e3 5e4 0e5 is non idempotent" >> {

          val id     = mkId
          val events = genEvents(6)

          for {
            _ <- streams.appendToStream(id, StreamState.NoStream, events)
            _ <- streams.appendToStream(id, StreamPosition.exact(5), Nel.one(events.head))
            e <- streams.readStreamForwards(id, maxCount = events.size + 2L).compile.toList

          } yield e.size shouldEqual events.size + 1

        }

        "sequence 0em1 1e0 2e1 3e2 4e3 5e4 0e6 raises" >> {

          val id     = mkId
          val events = genEvents(6)
          val result = for {
            _ <- streams.appendToStream(id, StreamState.NoStream, events)
            _ <- streams.appendToStream(id, StreamPosition.exact(6), Nel.one(events.head))
          } yield ()

          result.attempt.map {
            _ should beLeft(WrongExpectedState(id, StreamPosition.exact(6), StreamPosition.exact(5)))
          }
        }

        "sequence 0em1 1e0 2e1 3e2 4e3 5e4 0e4 raises" >> {

          val id     = mkId
          val events = genEvents(6)

          val result = for {
            _ <- streams.appendToStream(id, StreamState.NoStream, events)
            _ <- streams.appendToStream(id, StreamPosition.exact(4), Nel.one(events.head))
          } yield ()

          result.attempt.map {
            _ should beLeft(WrongExpectedState(id, StreamPosition.exact(4), StreamPosition.exact(5)))
          }
        }

        "sequence 0em1 0e0 is non idempotent" >> {

          val id     = mkId
          val events = genEvents(1)

          for {
            _ <- streams.appendToStream(id, StreamState.NoStream, events)
            _ <- streams.appendToStream(id, StreamPosition.Start, Nel.one(events.head))
            e <- streams.readStreamForwards(id, maxCount = events.size + 2L).compile.toList

          } yield e.size shouldEqual events.size + 1
        }

        "sequence 0em1" >> {

          def run(nextExpected: StreamState) = {

            val id     = mkId
            val events = genEvents(1)

            for {
              _ <- streams.appendToStream(id, StreamState.NoStream, events)
              _ <- streams.appendToStream(id, nextExpected, Nel.one(events.head))
              e <- streams.readStreamForwards(id, maxCount = events.size + 1L).compile.toList

            } yield e.size shouldEqual events.size
          }

          "0em1 is idempotent" >> {
            run(StreamState.NoStream)
          }

          "0any is idempotent" >> {
            run(StreamState.Any)
          }
        }

        "sequence 0em1 1e0 2e1 1any 1any is idempotent" >> {

          val id = mkId
          val e1 = genEvent
          val e2 = genEvent
          val e3 = genEvent

          for {
            _ <- streams.appendToStream(id, StreamState.NoStream, Nel.of(e1, e2, e3))
            _ <- streams.appendToStream(id, StreamState.Any, Nel.of(e2))
            _ <- streams.appendToStream(id, StreamState.Any, Nel.of(e2))
            e <- streams.readStreamForwards(id, maxCount = 4).compile.toList

          } yield e.size shouldEqual 3
        }

        "sequence S 0em1 1em1 E" >> {

          def run(nextState: StreamState, onlyLast: Boolean) = {

            val id         = mkId
            val e1         = genEvent
            val e2         = genEvent
            val events     = Nel.of(e1, e2)
            val nextEvents = if (onlyLast) Nel.one(e2) else events

            for {
              _ <- streams.appendToStream(id, StreamState.NoStream, events)
              _ <- streams.appendToStream(id, nextState, nextEvents)
              e <- streams.readStreamForwards(id, maxCount = events.size + 1L).compile.toList

            } yield e.size shouldEqual events.size
          }

          "S 0em1 E is idempotent" >> {
            run(StreamState.NoStream, onlyLast = false)
          }

          "S 0any E is idempotent" >> {
            run(StreamState.Any, onlyLast = false)
          }

          "S 1e0  E is idempotent" >> {
            run(StreamPosition.Start, onlyLast = true)
          }

          "S 1any E is idempotent" >> {
            run(StreamState.Any, onlyLast = true)
          }
        }

        "sequence S 0em1 1em1 E S 0em1 1em1 2em1 E raises" >> {

          val id     = mkId
          val e1     = genEvent
          val e2     = genEvent
          val e3     = genEvent
          val first  = Nel.of(e1, e2)
          val second = Nel.of(e1, e2, e3)

          streams.appendToStream(id, StreamState.NoStream, first) >> {
            streams.appendToStream(id, StreamState.NoStream, second).attempt.map {
              _ should beLeft(WrongExpectedState(id, StreamState.NoStream, StreamPosition.exact(1L)))
            }
          }
        }

      }

    }

    //==================================================================================================================

    "delete" >> {

      val streamPrefix = s"streams_delete_${genIdentifier}_"

      "a stream that does not exist" >> {

        def run(expectedState: StreamState) = {

          val st = mkSnakeCase(expectedState.show)
          val id = genStreamId(s"${streamPrefix}non_existing_stream_with_expected_state${st}_")

          streams.delete(id, expectedState).void
        }

        "works with no stream expected stream state" >> {
          run(StreamState.NoStream).as(ok)
        }

        "works with any expected stream state" >> {
          run(StreamState.Any).as(ok)
        }

        "raises with wrong expected stream state" >> {
          run(StreamPosition.Start).attempt.map {
            _ should beLike { case Left(WrongExpectedState(_, StreamPosition.Start, StreamState.NoStream)) => ok }
          }
        }

      }

      "a stream should return log position" >> {

        val id     = genStreamId(s"${streamPrefix}return_log_position_")
        val events = genEvents(1)

        for {
          wr  <- streams.appendToStream(id, StreamState.NoStream, events)
          pos <- streams.readStreamForwards(id, maxCount = 1).compile.lastOrError.map(_.logPosition)
          dr  <- streams.delete(id, wr.streamPosition)

        } yield dr.logPosition > pos

      }

      "a stream and reading raises" >> {

        val id     = genStreamId(s"${streamPrefix}reading_raises_")
        val events = genEvents(1)

        for {
          wr <- streams.appendToStream(id, StreamState.NoStream, events)
          _  <- streams.delete(id, wr.streamPosition)
          at <- streams.readStreamForwards(id, maxCount = 1).compile.drain.attempt

        } yield at should beLike { case Left(StreamNotFound(_)) => ok }

      }

      "a stream and tombstone works as expected" >> {

        val id     = genStreamId(s"${streamPrefix}and_tombstone_it_")
        val events = genEvents(2)

        for {
          wr  <- streams.appendToStream(id, StreamState.NoStream, events)
          _   <- streams.delete(id, wr.streamPosition)
          _   <- streams.tombstone(id, StreamState.Any)
          rat <- streams.readStreamForwards(id, maxCount = 2).compile.drain.attempt
          mat <- metaStreams.getMetadata(id).attempt
          aat <- streams.appendToStream(id, StreamState.Any, genEvents(1)).attempt

        } yield {
          rat should beLike { case Left(e: StreamDeleted) => e.streamId shouldEqual id.stringValue }
          mat should beLike { case Left(e: StreamDeleted) => e.streamId shouldEqual id.metaId.stringValue }
          aat should beLike { case Left(e: StreamDeleted) => e.streamId shouldEqual id.stringValue }
        }

      }

      "a stream and recreate with" >> {

        def run(expectedState: StreamState) = {

          val st           = mkSnakeCase(expectedState.show)
          val id           = genStreamId(s"${streamPrefix}and_recreate_with_expected_state_${st}_")
          val beforeEvents = genEvents(1)
          val afterEvents  = genEvents(3)

          for {
            wr1 <- streams.appendToStream(id, StreamState.NoStream, beforeEvents)
            _   <- streams.delete(id, wr1.streamPosition)
            wr2 <- streams.appendToStream(id, expectedState, afterEvents)
            _   <- IO.sleep(100.millis) // Workaround for ES github issue #1744
            evt <- streams.readStreamForwards(id, maxCount = 3).compile.toList
            tbm <- metaStreams.getTruncateBefore(id)

          } yield {

            wr1.streamPosition shouldEqual StreamPosition.Start
            wr2.streamPosition shouldEqual StreamPosition.exact(3)

            evt.size shouldEqual 3
            evt.map(_.eventData).toNel shouldEqual afterEvents.some
            evt.map(_.streamPosition) shouldEqual List(StreamPosition.exact(1),
                                                       StreamPosition.exact(2),
                                                       StreamPosition.exact(3))

            tbm should beSome(MetaStreams.Result(StreamPosition.exact(1), StreamPosition.exact(1).some))

          }

        }

        "no stream expected stream state" >> {
          run(StreamState.NoStream)
        }

        "any expected stream state" >> {
          run(StreamState.Any)
        }

        "exact expected stream state" >> {
          run(StreamPosition.Start)
        }

      }

      "a stream and recreate preserves metadata except truncate before" >> {

        val id           = genStreamId(s"${streamPrefix}and_recreate_preserves_metadata_")
        val beforeEvents = genEvents(2)
        val afterEvents  = genEvents(3)

        val metadata = StreamMetadata.empty
          .withAcl(StreamAcl.empty.withDeleteRoles(Set("some-role")))
          .withMaxCount(MaxCount(100).unsafe)
          .withTruncateBefore(StreamPosition.exact(Long.MaxValue))
          .withCustom("k1" -> Json.True, "k2" -> Json.fromInt(17), "k3" -> Json.fromString("some value"))

        for {
          swr    <- streams.appendToStream(id, StreamState.NoStream, beforeEvents)
          mwr    <- metaStreams.setMetadata(id, StreamState.NoStream, metadata)
          _      <- streams.appendToStream(id, StreamPosition.exact(1), afterEvents)
          _      <- IO.sleep(1.second) // Workaround for ES github issue #1744
          events <- streams.readStreamForwards(id, maxCount = 3).compile.toList
          meta   <- metaStreams.getMetadata(id)

        } yield {

          swr.streamPosition shouldEqual StreamPosition.exact(1)
          mwr.streamPosition shouldEqual StreamPosition.Start

          events.size shouldEqual afterEvents.size
          events.map(_.streamPosition) shouldEqual List(StreamPosition.exact(2),
                                                        StreamPosition.exact(3),
                                                        StreamPosition.exact(4))

          meta.fold(ko) { m =>
            m.streamPosition shouldEqual StreamPosition.exact(1)
            m.data shouldEqual metadata.withTruncateBefore(StreamPosition.exact(2))
          }
        }

      }

      "a stream and recreate raises if not first write" >> {

        val id = genStreamId(s"${streamPrefix}and_recreate_only_first_write_")

        for {
          wr1 <- streams.appendToStream(id, StreamState.NoStream, genEvents(2))
          _   <- streams.delete(id, wr1.streamPosition)
          wr2 <- streams.appendToStream(id, StreamState.NoStream, genEvents(3))
          wr3 <- streams.appendToStream(id, StreamState.NoStream, genEvents(1)).attempt
        } yield {

          wr1.streamPosition shouldEqual StreamPosition.exact(1)
          wr2.streamPosition shouldEqual StreamPosition.exact(4)
          wr3 should beLeft(WrongExpectedState(id, StreamState.NoStream, StreamPosition.exact(4L)))
        }

      }

      "a stream and recreate with multiple appends and expected stream state any" >> {

        val id      = genStreamId(s"${streamPrefix}and_recreate_multiple_writes_with_any_expected_state_")
        val events0 = genEvents(2)
        val events1 = genEvents(3)
        val events2 = genEvents(2)

        for {
          wr0    <- streams.appendToStream(id, StreamState.NoStream, events0)
          _      <- streams.delete(id, wr0.streamPosition)
          wr1    <- streams.appendToStream(id, StreamState.Any, events1)
          wr2    <- streams.appendToStream(id, StreamState.Any, events2)
          events <- streams.readStreamForwards(id, maxCount = 5).compile.toList
          tbr    <- metaStreams.getTruncateBefore(id)
        } yield {

          wr0.streamPosition shouldEqual StreamPosition.exact(1)
          wr1.streamPosition shouldEqual StreamPosition.exact(4)
          wr2.streamPosition shouldEqual StreamPosition.exact(6)

          events.size shouldEqual 5
          events.map(_.eventData) shouldEqual events1.concatNel(events2).toList
          events.map(_.streamPosition) shouldEqual (2L to 6L).map(StreamPosition.exact).toList

          tbr.fold(ko) { result =>
            result.data should beSome(StreamPosition.exact(2))
            result.streamPosition shouldEqual StreamPosition.exact(1)

          }
        }

      }

      "a stream and recreate on empty when metadata set" >> {

        val id = genStreamId(s"${streamPrefix}and_recreate_on_empty_when_metadata_set_")

        val metadata = StreamMetadata.empty
          .withMaxCount(MaxCount(100).unsafe)
          .withAcl(StreamAcl.empty.withDeleteRoles(Set("some-role")))
          .withTruncateBefore(StreamPosition.exact(Long.MaxValue))
          .withCustom("k1" -> Json.True, "k2" -> Json.fromInt(17), "k3" -> Json.fromString("some value"))

        for {
          _    <- streams.delete(id, StreamState.NoStream)
          mw   <- metaStreams.setMetadata(id, StreamPosition.Start, metadata)
          read <- streams.readStreamForwards(id, maxCount = 1).compile.toList.attempt
          meta <- metaStreams.getMetadata(id)
        } yield {

          mw.streamPosition shouldEqual StreamPosition.exact(1)

          read should beLike { case Left(e: StreamNotFound) =>
            e.streamId shouldEqual id.stringValue
          }

          meta.fold(ko) { m =>
            m.streamPosition shouldEqual StreamPosition.exact(2)
            m.data shouldEqual metadata.withTruncateBefore(StreamPosition.Start)
          }
        }

      }

      "a stream and recreate on non-empty when metadata set" >> {

        val id     = genStreamId(s"${streamPrefix}and_recreate_on_non_empty_when_metadata_set_")
        val events = genEvents(2)

        val metadata = StreamMetadata.empty
          .withMaxCount(MaxCount(100).unsafe)
          .withAcl(StreamAcl.empty.withDeleteRoles(Set("some-role")))
          .withCustom("k1" -> Json.True, "k2" -> Json.fromInt(17), "k3" -> Json.fromString("some value"))

        for {
          sw1  <- streams.appendToStream(id, StreamState.NoStream, events)
          _    <- streams.delete(id, sw1.streamPosition)
          mw1  <- metaStreams.setMetadata(id, StreamPosition.Start, metadata)
          read <- streams.readStreamForwards(id, maxCount = 2).compile.toList
          meta <- metaStreams.getMetadata(id)
        } yield {

          sw1.streamPosition shouldEqual StreamPosition.exact(1)
          mw1.streamPosition shouldEqual StreamPosition.exact(1)
          read.size shouldEqual 0

          meta.fold(ko) { m =>
            m.data shouldEqual metadata.withTruncateBefore(StreamPosition.exact(events.size.toLong))
          }
        }

      }

    }

    //==================================================================================================================

    "tombstone" >> {

      val streamPrefix = s"streams_tombstone_${genIdentifier}_"

      "a stream that does not exist" >> {

        def run(expectedState: StreamState) = {

          val st = mkSnakeCase(expectedState.show)
          val id = genStreamId(s"${streamPrefix}non_existing_stream_with_expected_state_${st}_")

          streams.tombstone(id, expectedState).void
        }

        "works with no stream expected stream state" >> {
          run(StreamState.NoStream).as(ok)
        }

        "works with any expected stream state" >> {
          run(StreamState.Any).as(ok)
        }

        "raises with wrong expected stream state" >> {
          run(StreamPosition.Start).attempt.map {
            _ should beLike { case Left(WrongExpectedState(_, StreamPosition.Start, StreamState.NoStream)) => ok }
          }
        }

      }

      "a stream should return log position" >> {

        val id     = genStreamId(s"${streamPrefix}return_log_position_")
        val events = genEvents(1)

        for {
          wr  <- streams.appendToStream(id, StreamState.NoStream, events)
          pos <- streams.readStreamForwards(id, maxCount = 1).compile.lastOrError.map(_.logPosition)
          dr  <- streams.tombstone(id, wr.streamPosition)

        } yield dr.logPosition > pos

      }

      "a tombstoned stream should raise" >> {
        val id = genStreamId(s"${streamPrefix}tombstoned_stream_")
        streams.tombstone(id, StreamState.NoStream) >> {
          streams.tombstone(id, StreamState.NoStream).attempt.map {
            _ should beLike { case Left(StreamDeleted(_)) => ok }
          }
        }
      }

    }

    //==================================================================================================================

  }
}
