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

import scala.concurrent.duration.*
import cats.syntax.all.*
import cats.effect.{IO, Ref}
import fs2.Stream
import sec.syntax.all.*
import sec.api.exceptions.*

class SubscribeToStreamSuite extends SnSuite:

  import StreamState.*
  import StreamPosition.*

  val streamPrefix                                       = s"streams_subscribe_to_stream_${genIdentifier}_"
  val fromBeginning: Option[StreamPosition]              = Option.empty
  val fromStreamPosition: Long => Option[StreamPosition] = r => StreamPosition(r).some
  val fromEnd: Option[StreamPosition]                    = End.some

  group("stream does not exist prior to subscribing") {

    val events = genEvents(50)

    def run(exclusivefrom: Option[StreamPosition], takeCount: Int) = {

      val id        = genStreamId(s"${streamPrefix}non_existing_stream_")
      val subscribe = streams.subscribeToStream(id, exclusivefrom).take(takeCount.toLong).map(_.eventData)
      val write     = Stream.eval(streams.appendToStream(id, NoStream, events)).delayBy(1.second)
      val result    = subscribe.concurrently(write)

      result.compile.toList
    }

    test("from beginning") {
      assertIO(run(fromBeginning, events.size), events.toList)
    }

    test("from stream position") {
      assertIO(run(fromStreamPosition(4), events.size - 5), events.toList.drop(5))
    }

    test("from end") {
      assertIO(run(fromEnd, events.size), events.toList)
    }

  }

  group("multiple subscriptions to same stream") {

    val eventCount      = 10
    val subscriberCount = 3
    val events          = genEvents(eventCount)

    def run(exclusivFrom: Option[StreamPosition], takeCount: Int) =

      val id    = genStreamId(s"${streamPrefix}multiple_subscriptions_to_same_stream_")
      val write = Stream.eval(streams.appendToStream(id, NoStream, events)).delayBy(1.second)

      def mkSubscribers(onEvent: IO[Unit]): Stream[IO, Event] = Stream
        .emit(streams.subscribeToStream(id, exclusivFrom).evalTap(_ => onEvent).take(takeCount.toLong))
        .repeat
        .take(subscriberCount.toLong)
        .parJoin(subscriberCount)

      val result: Stream[IO, Int] = Stream.eval(Ref.of[IO, Int](0)) >>= { ref =>
        Stream(mkSubscribers(ref.update(_ + 1)), write).parJoinUnbounded >> Stream.eval(ref.get)
      }

      assertIO(result.compile.lastOrError, takeCount * subscriberCount)

    test("from beginning")(run(fromBeginning, eventCount))

    test("from stream position")(run(fromStreamPosition(0), eventCount - 1))

    test("from end")(run(fromEnd, eventCount))

  }

  group("existing stream") {

    val beforeEvents = genEvents(40)
    val afterEvents  = genEvents(10)
    val totalEvents  = beforeEvents.concatNel(afterEvents)

    def run(exclusiveFrom: Option[StreamPosition], takeCount: Int) =

      val id = genStreamId(s"${streamPrefix}existing_and_new_")

      val beforeWrite =
        Stream.eval(streams.appendToStream(id, NoStream, beforeEvents))

      def afterWrite(st: StreamState): Stream[IO, WriteResult] =
        Stream.eval(streams.appendToStream(id, st, afterEvents)).delayBy(1.second)

      def subscribe(onEvent: Event => IO[Unit]): Stream[IO, Event] =
        streams.subscribeToStream(id, exclusiveFrom).evalTap(onEvent).take(takeCount.toLong)

      val result: Stream[IO, List[EventData]] = for
        ref        <- Stream.eval(Ref.of[IO, List[EventData]](Nil))
        st         <- beforeWrite.map(_.streamPosition)
        _          <- Stream.sleep[IO](1.second)
        _          <- subscribe(e => ref.update(_ :+ e.eventData)).concurrently(afterWrite(st))
        readEvents <- Stream.eval(ref.get)
      yield readEvents

      result.compile.lastOrError

    test("from beginning - reads all events and listens for new ones") {
      assertIO(run(fromBeginning, totalEvents.size).map(_.toNel), totalEvents.some)
    }

    test("from stream position - reads events after stream position and listens for new ones") {
      assertIO(run(fromStreamPosition(29), 20), totalEvents.toList.drop(30))
    }

    test("from end - listens for new events at given end of stream") {
      assertIO(run(fromEnd, afterEvents.size).map(_.toNel), afterEvents.some)
    }

  }

  group("raises when stream is tombstoned") {

    def run(exclusiveFrom: Option[StreamPosition]) = {

      val id        = genStreamId(s"${streamPrefix}stream_is_tombstoned_")
      val subscribe = streams.subscribeToStream(id, exclusiveFrom)
      val delete    = Stream.eval(streams.tombstone(id, Any)).delayBy(1.second)
      val expected  = StreamDeleted(id.stringValue).asLeft

      assertIO(subscribe.concurrently(delete).compile.last.attempt, expected)
    }

    test("from beginning")(run(fromBeginning))

    test("from stream position")(run(fromStreamPosition(5)))

    test("from end")(run(fromEnd))

  }
