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

import java.time.ZonedDateTime
import java.util.UUID
import java.{util => ju}
import cats.syntax.all._
import scodec.bits.ByteVector
import sec.arbitraries._
import sec.helpers.implicits._
import sec.helpers.text.encodeToBV

//======================================================================================================================

class EventSuite extends SecSuite {

  private def bv(data: String): ByteVector =
    ByteVector.encodeUtf8(data).leftMap(_.getMessage).unsafe

  val er: EventRecord[PositionInfo.Global] = sec.EventRecord[PositionInfo.Global](
    StreamId("abc-1234").unsafe,
    PositionInfo.Global(StreamPosition(5L), LogPosition.exact(42L, 42L)),
    EventData("et", sampleOf[ju.UUID], bv("abc"), ContentType.Binary).unsafe,
    sampleOf[ZonedDateTime]
  )

  val link: EventRecord[PositionInfo.Global] = sec.EventRecord[PositionInfo.Global](
    StreamId.system("ce-abc").unsafe,
    PositionInfo.Global(StreamPosition(10L), LogPosition.exact(1337L, 1337L)),
    EventData(EventType.LinkTo, sampleOf[ju.UUID], bv("5@abc-1234"), ContentType.Binary),
    sampleOf[ZonedDateTime]
  )

  val re: ResolvedEvent[PositionInfo.Global] = ResolvedEvent(er, link)

  //

  group("EventOps") {

    test("fold") {
      assert(er.fold(_ => true, _ => false))
      assert(re.fold(_ => false, _ => true))
    }

    def testCommon[P <: PositionInfo](er: EventRecord[P], re: ResolvedEvent[P]) = {

      test("streamId") {
        assertEquals((er: Event[P]).streamId, er.streamId)
        assertEquals((re: Event[P]).streamId, er.streamId)
      }

      test("streamPosition") {
        assertEquals((er: Event[P]).streamPosition, er.streamPosition)
        assertEquals((re: Event[P]).streamPosition, er.streamPosition)
      }

      test("eventData") {
        assertEquals((er: Event[P]).eventData, er.eventData)
        assertEquals((re: Event[P]).eventData, er.eventData)
      }

      test("record") {
        assertEquals((er: Event[P]).record, er)
        assertEquals((re: Event[P]).record, re.link)
      }

      test("created") {
        assertEquals((er: Event[P]).created, er.created)
        assertEquals((re: Event[P]).created, er.created)
      }
    }

    testCommon(er, re)
    testCommon(
      er.copy(position = er.position.streamPosition),
      re.copy(event    = re.event.copy(position = re.event.position.streamPosition),
              link     = re.link.copy(position = re.link.position.streamPosition))
    )

    test("logPosition") {
      assertEquals((er: Event[PositionInfo.Global]).logPosition, er.logPosition)
      assertEquals((re: Event[PositionInfo.Global]).logPosition, er.logPosition)
    }

    test("render") {

      assertEquals(
        er.render,
        s"""
        |EventRecord(
        |  streamId = ${er.streamId.render},
        |  eventId  = ${er.eventData.eventId},
        |  type     = ${er.eventData.eventType.render},
        |  position = ${er.position.renderPosition},
        |  data     = ${er.eventData.renderData},
        |  metadata = ${er.eventData.renderMetadata},
        |  created  = ${er.created}
        |)
        |""".stripMargin
      )

      assertEquals(
        re.render,
        s"""
        |ResolvedEvent(
        |  event = ${re.event.render},
        |  link  = ${re.link.render}
        |)
        |""".stripMargin
      )

    }

  }

}

//======================================================================================================================

class EventTypeSuite extends SecSuite {

  val normal: EventType = EventType.Normal.unsafe("user")
  val system: EventType = EventType.System.unsafe("system")

  test("apply") {
    assertEquals(EventType(""), Left(InvalidInput("Event type name cannot be empty")))
    assertEquals(EventType("$users"), Left(InvalidInput("value must not start with $, but is $users")))
    assertEquals(EventType("users"), Right(EventType.normal("users").unsafe))
  }

  test("eventTypeToString") {
    assertEquals(EventType.eventTypeToString(EventType.StreamDeleted), "$streamDeleted")
    assertEquals(EventType.eventTypeToString(EventType.StatsCollected), "$statsCollected")
    assertEquals(EventType.eventTypeToString(EventType.LinkTo), "$>")
    assertEquals(EventType.eventTypeToString(EventType.StreamReference), "$@")
    assertEquals(EventType.eventTypeToString(EventType.StreamMetadata), "$metadata")
    assertEquals(EventType.eventTypeToString(EventType.Settings), "$settings")
    assertEquals(EventType.eventTypeToString(normal), "user")
    assertEquals(EventType.eventTypeToString(system), s"$$system")
  }

  test("stringToEventType") {
    assertEquals(EventType.stringToEventType("$streamDeleted"), EventType.StreamDeleted.asRight)
    assertEquals(EventType.stringToEventType("$statsCollected"), EventType.StatsCollected.asRight)
    assertEquals(EventType.stringToEventType("$>"), EventType.LinkTo.asRight)
    assertEquals(EventType.stringToEventType("$@"), EventType.StreamReference.asRight)
    assertEquals(EventType.stringToEventType("$metadata"), EventType.StreamMetadata.asRight)
    assertEquals(EventType.stringToEventType("$settings"), EventType.Settings.asRight)
    assertEquals(EventType.stringToEventType(normal.stringValue), Right(normal))
    assertEquals(EventType.stringToEventType(system.stringValue), Right(system))
  }

  group("EventTypeOps") {

    test("render") {
      val et = sampleOf[EventType]
      assertEquals(et.render, et.stringValue)
    }

    test("stringValue") {
      val et = sampleOf[EventType]
      assertEquals(et.stringValue, EventType.eventTypeToString(et))
    }

  }

}

//======================================================================================================================

class EventDataSuite extends SecSuite {

  import ContentType.{Binary, Json}

  def encode(data: String): ByteVector =
    encodeToBV(data).unsafe

  val bve: ByteVector      = ByteVector.empty
  val et: EventType.Normal = EventType("eventType").unsafe
  val id: UUID             = sampleOf[ju.UUID]

  val dataJson: ByteVector   = encode("""{ "data": "1" }""")
  val metaJson: ByteVector   = encode("""{ "meta": "2" }""")
  val dataBinary: ByteVector = encode("data")
  val metaBinary: ByteVector = encode("meta")

  test("apply") {

    val errEmpty = InvalidInput("Event type name cannot be empty")
    val errStart = InvalidInput("value must not start with $, but is $system")

    def testCommon(data: ByteVector, meta: ByteVector, ct: ContentType) = {

      assertEquals(EventData("", id, data, ct), Left(errEmpty))
      assertEquals(EventData("", id, data, meta, ct), Left(errEmpty))
      assertEquals(EventData("$system", id, data, ct), Left(errStart))
      assertEquals(EventData("$system", id, data, meta, ct), Left(errStart))

      val ed1 = EventData(et, id, data, ct)

      ed1 match {
        case ed @ EventData(`et`, `id`, `data`, `bve`, `ct`) => assertEquals(ed1, ed)
        case other                                           => fail(s"Did not expect $other")
      }

      val ed2 = EventData(et, id, data, meta, ct)

      ed2 match {
        case ed @ EventData(`et`, `id`, `data`, `meta`, `ct`) => assertEquals(ed2, ed)
        case other                                            => fail(s"Did not expect $other")
      }
    }

    // /

    testCommon(dataJson, metaJson, Json)
    testCommon(dataBinary, metaBinary, Binary)
  }

  group("EventData") {

    test("render") {
      assertEquals(EventData.render(bve, Json), "Json(empty)")
      assertEquals(EventData.render(bve, Binary), "Binary(empty)")
      assertEquals(EventData.render(encode("""{ "link" : "1@a" }"""), Json), """{ "link" : "1@a" }""")
      assertEquals(EventData.render(encode("a"), Binary), "Binary(1 bytes, 0x61)")
      assertEquals(EventData.render(encode("a" * 31), Binary), s"Binary(31 bytes, 0x${"61" * 31})")
      assertEquals(EventData.render(encode("a" * 32), Binary), "Binary(32 bytes, #-547736941)")
    }

  }

}

//======================================================================================================================

class ContentTypeSuite extends SecSuite {

  import ContentType._

  test("render") {
    assertEquals((Binary: ContentType).render, "Binary")
    assertEquals((Json: ContentType).render, "Json")
  }

  group("ContentTypeOps") {

    test("fold") {
      assertEquals(Binary.fold("b", "j"), "b")
      assertEquals(Json.fold("b", "j"), "j")
    }

    test("isJson") {
      assert(Json.isJson)
      assertNot(Binary.isJson)
    }

    test("isBinary") {
      assert(Binary.isBinary)
      assertNot(Json.isBinary)
    }

  }

}

//======================================================================================================================
