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
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import cats.effect.IO
import cats.effect.concurrent.Ref
import fs2.Stream
import sec.core._
import sec.api.Direction._
import sec.api.exceptions._

class StreamsSpec extends SnSpec {

  sequential

  "Streams" should {

    //==================================================================================================================

    "subscribeToAll" >> {
      ok
    }

    "subscribeToStream" >> {

      val streamPrefix                              = s"streams_subscribe_to_stream_${genIdentifier}_"
      val fromBeginning: Option[EventNumber]        = Option.empty
      val fromRevision: Long => Option[EventNumber] = r => EventNumber.exact(r).some
      val fromEnd: Option[EventNumber]              = EventNumber.End.some

      "works when stream does not exist prior to subscribing" >> {

        val events = genEvents(50)

        def test(exclusivefrom: Option[EventNumber], takeCount: Int) = {

          val id        = genStreamId(s"${streamPrefix}non_existing_stream")
          val subscribe = streams.subscribeToStream(id, exclusivefrom).take(takeCount.toLong).map(_.eventData)
          val write     = Stream.eval(streams.appendToStream(id, StreamRevision.NoStream, events, None)).delayBy(300.millis)
          val result    = subscribe.concurrently(write)

          result.compile.toList
        }

        "from beginning" >> {
          test(fromBeginning, events.size).map(_ shouldEqual events.toList)
        }

        "from revision" >> {
          test(fromRevision(4), events.size - 5).map(_ shouldEqual events.toList.drop(5))
        }

        "from end" >> {
          test(fromEnd, events.size).map(_ shouldEqual events.toList)
        }

      }

      "works with multiple subscriptions to same stream" >> {

        val eventCount      = 10
        val subscriberCount = 4
        val events          = genEvents(eventCount)

        def test(exclusivFrom: Option[EventNumber], takeCount: Int) = {

          val id    = genStreamId(s"${streamPrefix}multiple_subscriptions_to_same_stream")
          val write = Stream.eval(streams.appendToStream(id, StreamRevision.NoStream, events, None)).delayBy(300.millis)

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

        "from revision" >> {
          test(fromRevision(0), eventCount - 1)
        }

        "from end" >> {
          test(fromEnd, eventCount)
        }

      }

      "works with existing stream" >> {

        val beforeEvents = genEvents(40)
        val afterEvents  = genEvents(10)
        val totalEvents  = beforeEvents.concatNel(afterEvents)

        def test(exclusiveFrom: Option[EventNumber], takeCount: Int) = {

          val id = genStreamId(s"${streamPrefix}existing_and_new")

          val beforeWrite =
            Stream.eval(streams.appendToStream(id, StreamRevision.NoStream, beforeEvents, None))

          def afterWrite(rev: StreamRevision): Stream[IO, Streams.WriteResult] =
            Stream.eval(streams.appendToStream(id, rev, afterEvents, None)).delayBy(300.millis)

          def subscribe(onEvent: Event => IO[Unit]): Stream[IO, Event] =
            streams.subscribeToStream(id, exclusiveFrom).evalTap(onEvent).take(takeCount.toLong)

          val result: Stream[IO, List[EventData]] = for {
            ref        <- Stream.eval(Ref.of[IO, List[EventData]](Nil))
            rev        <- beforeWrite.map(_.currentRevision)
            _          <- subscribe(e => ref.update(_ :+ e.eventData)).concurrently(afterWrite(rev))
            readEvents <- Stream.eval(ref.get)
          } yield readEvents

          result.compile.lastOrError

        }

        "from beginning - reads all events and listens for new ones" >> {
          test(fromBeginning, totalEvents.size).map(_.toNel shouldEqual totalEvents.some)
        }

        "from revision - reads events after revision and listens for new ones" >> {
          test(fromRevision(29), 20).map(_ shouldEqual totalEvents.toList.drop(30))
        }

        "from end - listens for new events at given end of stream" >> {
          test(fromEnd, afterEvents.size).map(_.toNel shouldEqual afterEvents.some)
        }

      }

      "raises when stream is hard deleted (tombstoned)" >> {

        def test(exclusiveFrom: Option[EventNumber]) = {

          val id        = genStreamId(s"${streamPrefix}stream_gets_hard_deleted")
          val subscribe = streams.subscribeToStream(id, exclusiveFrom)
          val delete    = Stream.eval(streams.hardDelete(id, StreamRevision.Any)).delayBy(300.millis)
          val expected  = StreamDeleted(id.stringValue).asLeft

          subscribe.concurrently(delete).compile.last.attempt.map(_.shouldEqual(expected))
        }

        "from beginning" >> {
          test(fromBeginning)
        }

        "from revision" >> {
          test(fromRevision(5))
        }

        "from end" >> {
          test(fromEnd)
        }

      }

    }

    //==================================================================================================================

    "readAll" >> {

      import Position._

      val streamPrefix    = s"streams_read_all_${genIdentifier}_"
      val eventTypePrefix = s"sec.$genIdentifier.Event"

      val id1     = genStreamId(streamPrefix)
      val id2     = genStreamId(streamPrefix)
      val events1 = genEvents(500, eventTypePrefix)
      val events2 = genEvents(500, eventTypePrefix)
      val written = events1 ::: events2

      val writeEvents =
        streams.appendToStream(id1, StreamRevision.NoStream, events1, None) *>
          streams.appendToStream(id2, StreamRevision.NoStream, events2, None)

      def read(position: Position, direction: Direction, maxCount: Long = 1) =
        streams.readAll(position, direction, maxCount, resolveLinkTos = false, None).compile.toList

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
            .readAll(Start, Forwards, events1.length / 2L, resolveLinkTos = false, None)
            .take(events1.length.toLong)
            .compile
            .toList
            .map(_.size shouldEqual events1.length / 2)
        }

        def deleted(soft: Boolean) = {

          val suffix = if (soft) "soft" else "hard"
          val id     = genStreamId(s"streams_read_all_deleted_$suffix")
          val events = genEvents(10)
          val write  = streams.appendToStream(id, StreamRevision.NoStream, events, None)

          def delete(er: EventNumber.Exact) =
            if (soft) streams.softDelete(id, er, None) else streams.hardDelete(id, er, None)

          val decodeJson: EventRecord => IO[StreamMetadata] =
            _.eventData.data.bytes.decodeUtf8.liftTo[IO] >>= {
              io.circe.parser.decode[StreamMetadata](_).liftTo[IO]
            }

          val verifyDeleted =
            streams.readStreamForwards(id, EventNumber.Start, 1).compile.drain.recoverWith {
              case e: StreamNotFound if soft && e.streamId.eqv(id.stringValue) => IO.unit
              case e: StreamDeleted if !soft && e.streamId.eqv(id.stringValue) => IO.unit
            }

          val read = streams
            .readAllForwards(Start, Int.MaxValue - 1)
            .filter(e => e.streamId.eqv(id) || e.streamId.eqv(id.metaId))
            .compile
            .toList

          val setup = write >>= { wr => delete(wr.currentRevision).void >> verifyDeleted >> read }

          def verify(es: List[Event]) =
            es.lastOption.toRight(new RuntimeException("expected metadata")).liftTo[IO] >>= { ts =>
              if (soft) {
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

        "soft deleted stream" >> deleted(soft = true)
        "hard deleted stream" >> deleted(soft = false)

        "max count deleted events are not resolved" >> {

          val deletedId = genStreamId("streams_read_all_linkto_deleted_")
          val linkId    = genStreamId("streams_read_all_linkto_link_")
          val maxCount  = MaxCount.from(2).unsafe

          def linkData(number: Long) =
            Nel.one(
              EventData.binary(
                EventType.LinkTo,
                ju.UUID.randomUUID(),
                Content.binary(s"$number@${deletedId.stringValue}").unsafe.bytes,
                ByteVector.empty
              ))

          def append(id: StreamId, data: Nel[EventData]) =
            streams.appendToStream(id, StreamRevision.Any, data, None)

          def readLink(resolve: Boolean) =
            streams.readStreamForwards(linkId, EventNumber.Start, 3, resolve, None).map(_.eventData).compile.toList

          val e1 = genEvents(1)
          val e2 = genEvents(1)
          val e3 = genEvents(1)

          val l1 = linkData(0)
          val l2 = linkData(1)
          val l3 = linkData(2)

          append(deletedId, e1) >>
            streams.metadata.setMaxCount(deletedId, maxCount, None, None) >>
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
            .readAll(End, Backwards, events1.length / 2L, resolveLinkTos = false, None)
            .take(events1.length.toLong)
            .compile
            .toList
            .map(_.size shouldEqual events1.length / 2)
        }

      }

    }

    //==================================================================================================================

    "readStream" >> {

      import EventNumber._

      val streamPrefix = s"streams_read_stream_${genIdentifier}_"
      val id           = genStreamId(streamPrefix)
      val events       = genEvents(25)
      val writeEvents  = streams.appendToStream(id, StreamRevision.NoStream, events, None)

      def read(id: StreamId, from: EventNumber, direction: Direction, count: Long = 50) =
        streams.readStream(id, from, direction, count, resolveLinkTos = false, None).compile.toList

      def readData(id: StreamId, from: EventNumber, direction: Direction, count: Long = 50) =
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

        "reading soft deleted stream raises stream not found" >> {

          val id = genStreamId(s"${streamPrefix}stream_soft_deleted_forwards_")
          val ex = StreamNotFound(id.stringValue)

          streams.appendToStream(id, StreamRevision.NoStream, genEvents(5), None) *>
            streams.softDelete(id, StreamRevision.Any, None) *>
            read(id, Start, Forwards, 5).attempt.map(_ should beLeft(ex))
        }

        "reading hard deleted stream raises stream deleted" >> {

          val id = genStreamId(s"${streamPrefix}stream_hard_deleted_forwards_")
          val ex = StreamDeleted(id.stringValue)

          streams.appendToStream(id, StreamRevision.NoStream, genEvents(5), None) *>
            streams.hardDelete(id, StreamRevision.Any, None) *>
            read(id, Start, Forwards, 5).attempt.map(_ should beLeft(ex))
        }

        "reading single event from arbitrary position" >> {
          readData(id, exact(7), Forwards, 1).map(_.lastOption shouldEqual events.get(7))
        }

        "reading from arbitrary position" >> {
          readData(id, exact(3L), Forwards, 2).map(_ shouldEqual events.toList.slice(3, 5))
        }

        "max count is respected" >> {
          streams
            .readStream(id, Start, Forwards, events.length / 2L, resolveLinkTos = false, None)
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

        "reading soft deleted stream raises stream not found" >> {

          val id = genStreamId(s"${streamPrefix}stream_soft_deleted_backwards_")
          val ex = StreamNotFound(id.stringValue)

          streams.appendToStream(id, StreamRevision.NoStream, genEvents(5), None) *>
            streams.softDelete(id, StreamRevision.Any, None) *>
            read(id, End, Backwards, 5).attempt.map(_ should beLeft(ex))
        }

        "reading hard deleted stream raises stream deleted" >> {

          val id = genStreamId(s"${streamPrefix}stream_hard_deleted_backwards_")
          val ex = StreamDeleted(id.stringValue)

          streams.appendToStream(id, StreamRevision.NoStream, genEvents(5), None) *>
            streams.hardDelete(id, StreamRevision.Any, None) *>
            read(id, End, Backwards, 5).attempt.map(_ should beLeft(ex))
        }

        "reading single event from arbitrary position" >> {
          readData(id, EventNumber.exact(20), Backwards, 1).map(_.lastOption shouldEqual events.get(20))
        }

        "reading from arbitrary position" >> {
          readData(id, EventNumber.exact(3L), Backwards, 2).map(_ shouldEqual events.toList.slice(2, 4).reverse)
        }

        "max count is respected" >> {
          streams
            .readStream(id, End, Backwards, events.length / 2L, resolveLinkTos = false, None)
            .take(events.length.toLong)
            .compile
            .toList
            .map(_.size shouldEqual events.length / 2)
        }

      }

    }

    //==================================================================================================================

    "appendToStream" >> {

      import helpers.text.{snakeCaseTransformation => sct}

      val streamPrefix = s"streams_append_to_stream_${genIdentifier}_"

      "create stream on first write if does not exist" >> {

        val events = genEvents(1)

        def test(expectedRevision: StreamRevision) = {

          val id = genStreamId(s"${streamPrefix}non_present_${sct(expectedRevision.show)}")

          streams.appendToStream(id, expectedRevision, events) >>= { wr =>
            streams.readStreamForwards(id, EventNumber.Start, 2).compile.toList.map { el =>
              el.map(_.eventData).toNel shouldEqual events.some
              wr.currentRevision shouldEqual EventNumber.Start
            }
          }
        }

        "works with any expected stream revision" >> {
          test(StreamRevision.Any)
        }

        "works with no stream expected stream revision" >> {
          test(StreamRevision.NoStream)
        }

        "raises with exact expected stream revision" >> {
          test(EventNumber.Start).attempt.map {
            _ should beLike { case Left(WrongExpectedVersion(_, Some(0L), None)) => ok }
          }
        }

        "raises with stream exists expected stream revision" >> {
          test(StreamRevision.StreamExists).attempt.map {
            _ should beLike { case Left(WrongExpectedVersion(_, None, None)) => ok }
          }
        }

      }

      "multiple idempotent writes" >> {

        "with unique uuids" >> {

          val id     = genStreamId(s"${streamPrefix}multiple_idempotent_writes")
          val events = genEvents(4)
          val write  = streams.appendToStream(id, StreamRevision.Any, events)

          write >>= { first =>
            write.map { second =>
              first.currentRevision shouldEqual second.currentRevision
              first.currentRevision shouldEqual EventNumber.exact(3L)
            }
          }

        }

        "with same uuids (bug in ESDB)" >> {

          val id     = genStreamId(s"${streamPrefix}multiple_idempotent_writes_same_uuid")
          val event  = genEvents(1).head
          val events = Nel.of(event, List.fill(5)(event): _*)
          val write  = streams.appendToStream(id, StreamRevision.Any, events)

          write.map(_.currentRevision shouldEqual EventNumber.exact(5))
        }

      }

      "multiple writes of multiple events with same uuids using expected stream revision" >> {

        def test(expectedRevision: StreamRevision, expectedSecondRevision: StreamRevision) = {

          val revision = sct(expectedRevision.show)
          val id       = genStreamId(s"${streamPrefix}multiple_writes_multiple_events_same_uuid_$revision")
          val event    = genEvents(1).head
          val events   = Nel.of(event, List.fill(5)(event): _*)
          val write    = streams.appendToStream(id, expectedRevision, events)

          write >>= { first =>
            write.map { second =>
              first.currentRevision shouldEqual EventNumber.exact(5)
              second.currentRevision shouldEqual expectedSecondRevision
            }
          }

        }

        "any then next expected revision is unreliable" >> {
          test(StreamRevision.Any, EventNumber.Start)
        }

        "no stream then next expected revision is correct" >> {
          test(StreamRevision.NoStream, EventNumber.exact(5))
        }

      }

      "append to hard deleted stream raises" >> {

        def test(expectedRevision: StreamRevision) = {
          val revStr = sct(expectedRevision.show)
          val id     = genStreamId(s"${streamPrefix}hard_deleted_stream_$revStr")
          val events = genEvents(1)
          val delete = streams.hardDelete(id, StreamRevision.NoStream)
          val write  = streams.appendToStream(id, expectedRevision, events)

          delete >> write.attempt.map(_ shouldEqual StreamDeleted(id.stringValue).asLeft)
        }

        "with correct expected revision" >> {
          test(StreamRevision.NoStream)
        }

        "with any expected revision" >> {
          test(StreamRevision.Any)
        }

        "with stream exists expected revision" >> {
          test(StreamRevision.StreamExists)
        }

        "with incorrect expected revision" >> {
          test(EventNumber.exact(5))
        }

      }

      "append to existing stream" >> {

        def test(sndExpectedRevision: StreamRevision) = {

          val revStr                     = sct(sndExpectedRevision.show)
          val id                         = genStreamId(s"${streamPrefix}existing_stream_with_$revStr")
          def write(esr: StreamRevision) = streams.appendToStream(id, esr, genEvents(1))

          write(StreamRevision.NoStream) >>= { first =>
            write(sndExpectedRevision).map { second =>
              first.currentRevision shouldEqual EventNumber.Start
              second.currentRevision
            }
          }
        }

        "works with correct expected revision" >> {
          test(EventNumber.Start).map(_ shouldEqual EventNumber.exact(1))
        }

        "works with any expected revision" >> {
          test(StreamRevision.Any).map(_ shouldEqual EventNumber.exact(1))
        }

        "works with stream exists expected revision" >> {
          test(StreamRevision.StreamExists).map(_ shouldEqual EventNumber.exact(1))
        }

        "raises with incorrect expected revision" >> {
          test(EventNumber.exact(1)).attempt.map {
            _ should beLike { case Left(WrongExpectedVersion(_, Some(1L), Some(0L))) => ok }
          }
        }

      }

      "append to stream with multiple events and stream exists expected revision " >> {

        val id      = genStreamId(s"${streamPrefix}multiple_events_and_stream_exists")
        val events  = genEvents(5)
        val writes  = events.toList.map(e => streams.appendToStream(id, StreamRevision.Any, Nel.one(e)))
        val prepare = Stream.eval(writes.sequence).compile.drain

        prepare >> streams
          .appendToStream(id, StreamRevision.StreamExists, genEvents(1))
          .map(_.currentRevision shouldEqual EventNumber.exact(5))

      }

      "append to stream with stream exists expected version works if metadata stream exists" >> {

        val id    = genStreamId(s"${streamPrefix}stream_exists_and_metadata_stream_exists")
        val meta  = MaxAge[IO](10.seconds) >>= { ma => streams.metadata.setMaxAge(id, ma, None, None) }
        val write = streams.appendToStream(id, StreamRevision.StreamExists, genEvents(1))

        meta >> write.map(_.currentRevision shouldEqual EventNumber.Start)

      }

      "append to soft deleted stream" >> {

        def test(expectedRevision: StreamRevision) = {

          val revStr = sct(expectedRevision.show)
          val id     = genStreamId(s"${streamPrefix}stream_exists_and_soft_deleted${revStr}")

          streams.softDelete(id, StreamRevision.NoStream) >>
            streams.appendToStream(id, expectedRevision, genEvents(1))
        }

        "with stream exists expected version raises" >> {
          test(StreamRevision.StreamExists).attempt.map {
            _ should beLike { case Left(StreamDeleted(_)) => ok }
          }
        }

      }

      "can append multiple events at once" >> {

        val id       = genStreamId(s"${streamPrefix}multiple_events_at_once")
        val events   = genEvents(100)
        val expected = EventNumber.exact(99)

        streams.appendToStream(id, StreamRevision.NoStream, events).map(_.currentRevision shouldEqual expected)

      }

      "append events with size" >> {

        val max = 1024 * 1024 // Default ESDB setting

        def mkEvent(sizeBytes: Int): IO[EventData] = IO(UUID.randomUUID()).map { uuid =>
          EventData.binary(EventType("et").unsafe, uuid, ByteVector.fill(sizeBytes.toLong)(0), ByteVector.empty)
        }

        "less than or equal max append size works" >> {

          val id       = genStreamId(s"${streamPrefix}append_size_less_or_equal_bytes")
          val equal    = List(mkEvent(max / 2), mkEvent(max / 2)).sequence.map(Nel.fromListUnsafe)
          val lessThan = List(mkEvent(max / 4), mkEvent(max / 2)).sequence.map(Nel.fromListUnsafe)

          def run(rev: StreamRevision)(data: Nel[EventData]) =
            streams.appendToStream(id, rev, data).as(ok)

          (equal >>= run(StreamRevision.NoStream)) >> (lessThan >>= run(EventNumber.exact(1)))

        }

        "greater than max append size raises" >> {

          val id          = genStreamId(s"${streamPrefix}append_size_exceeds_bytes")
          val greaterThan = List(mkEvent(max / 2), mkEvent(max / 2), mkEvent(max + 1)).sequence.map(Nel.fromListUnsafe)

          greaterThan >>= { events =>
            streams.appendToStream(id, StreamRevision.NoStream, events).attempt.map {
              _ shouldEqual MaximumAppendSizeExceeded(max.some).asLeft
            }
          }

        }

      }

    }

    //==================================================================================================================

  }
}
