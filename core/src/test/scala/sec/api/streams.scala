package sec
package api

import java.{util => ju}
import scala.concurrent.duration._
import scodec.bits.ByteVector
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect.IO
import fs2.Stream
import sec.core._
import sec.api.Direction._

class StreamsITest extends ITest {

  sequential

  "Streams" >> {

    //======================================================================================================================

    "readAll" >> {

      import Position._

      val streamPrefix    = s"streams_read_all_${genIdentifier}_"
      val eventTypePrefix = s"sec.${genIdentifier}.Event"

      val id1     = genStreamId(streamPrefix)
      val id2     = genStreamId(streamPrefix)
      val events1 = genEvents(500, eventTypePrefix)
      val events2 = genEvents(500, eventTypePrefix)
      val written = events1 ::: events2

      val writeEvents =
        streams.appendToStream(id1, StreamRevision.NoStream, events1, None) *>
          streams.appendToStream(id2, StreamRevision.NoStream, events2, None)

      def read(position: Position, direction: Direction, maxCount: Long = 1) =
        streams.readAll(position, direction, maxCount, false, None).compile.toList

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
            .readAll(Start, Forwards, events1.length / 2L, false, None)
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
            es.lastOption.toRight(ValidationError("expected metadata")).liftTo[IO] >>= { ts =>
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
            NonEmptyList.one(
              EventData.binary(
                EventType.LinkTo,
                ju.UUID.randomUUID(),
                Content.binary(s"$number@${deletedId.stringValue}").unsafe.bytes,
                ByteVector.empty
              ))

          def append(id: StreamId, data: NonEmptyList[EventData]) =
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
            meta.setMaxCount(deletedId, maxCount, None, None) >>
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
            .readAll(End, Backwards, events1.length / 2L, false, None)
            .take(events1.length.toLong)
            .compile
            .toList
            .map(_.size shouldEqual events1.length / 2)
        }

      }

    }

    //======================================================================================================================

    "readStream" >> {

      import EventNumber._

      val streamPrefix = s"streams_read_stream_${genIdentifier}_"
      val id           = genStreamId(streamPrefix)
      val events       = genEvents(25)
      val writeEvents  = streams.appendToStream(id, StreamRevision.NoStream, events, None)

      def read(id: StreamId, from: EventNumber, direction: Direction, count: Long = 50) =
        streams.readStream(id, from, direction, count, false, None).compile.toList

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
          readData(id, exact(3L), Forwards, 2).map(_ shouldEqual events.toList.drop(3).take(2))
        }

        "max count is respected" >> {
          streams
            .readStream(id, Start, Forwards, events.length / 2L, false, None)
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
          readData(id, EventNumber.exact(3L), Backwards, 2).map(_ shouldEqual events.toList.drop(2).take(2).reverse)
        }

        "max count is respected" >> {
          streams
            .readStream(id, End, Backwards, events.length / 2L, false, None)
            .take(events.length.toLong)
            .compile
            .toList
            .map(_.size shouldEqual events.length / 2)
        }

      }

    }

    //======================================================================================================================

    "subscribeToStream" >> {

      "works when stream does not exist prior to subscribing" >> {

        val streamPrefix = s"streams_subscribe_to_stream_${genIdentifier}_"
        val events       = genEvents(50)
        val id           = genStreamId(s"${streamPrefix}non_existing_stream_")
        val subscribe    = streams.subscribeToStream(id, None).take(50).map(_.eventData)
        val write        = Stream.eval(streams.appendToStream(id, StreamRevision.NoStream, events, None)).delayBy(300.millis)
        val result       = subscribe.concurrently(write)

        result.compile.toList.map(_ shouldEqual events.toList)

      }

    }

  }
}
