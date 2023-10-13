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
import java.util as ju
import cats.syntax.all.*
import scodec.bits.ByteVector
import sec.arbitraries.{*, given}
import sec.helpers.implicits.*
import sec.helpers.text.encodeToBV

//======================================================================================================================

class EventSuite extends SecSuite:

  private def bv(data: String): ByteVector =
    ByteVector.encodeUtf8(data).leftMap(_.getMessage).unsafe

  val er: EventRecord = sec.EventRecord(
    StreamId("abc-1234").unsafeGet,
    StreamPosition(5L),
    LogPosition.exact(42L, 42L),
    EventData("et", sampleOf[ju.UUID], bv("abc"), ContentType.Binary).unsafeGet,
    sampleOf[ZonedDateTime]
  )

  val link: EventRecord = sec.EventRecord(
    StreamId.system("ce-abc").unsafe,
    StreamPosition(10L),
    LogPosition.exact(1337L, 1337L),
    EventData(EventType.LinkTo, sampleOf[ju.UUID], bv("5@abc-1234"), ContentType.Binary),
    sampleOf[ZonedDateTime]
  )

  val re: ResolvedEvent = ResolvedEvent(er, link)

  //

  group("EventOps") {

    test("fold") {
      assert(er.fold(_ => true, _ => false))
      assert(re.fold(_ => false, _ => true))
    }

    def testCommon(er: EventRecord, re: ResolvedEvent) = {

      test("streamId") {
        assertEquals(er.streamId, er.streamId)
        assertEquals(re.streamId, er.streamId)
      }

      test("streamPosition") {
        assertEquals(er.streamPosition, er.streamPosition)
        assertEquals(re.streamPosition, er.streamPosition)
      }

      test("eventData") {
        assertEquals(er.eventData, er.eventData)
        assertEquals(re.eventData, er.eventData)
      }

      test("record") {
        assertEquals(er.record, er)
        assertEquals(re.record, re.link)
      }

      test("created") {
        assertEquals(er.created, er.created)
        assertEquals(re.created, er.created)
      }
    }

    testCommon(er, re)
    testCommon(
      er.copy(streamPosition = er.streamPosition),
      re.copy(event          = re.event.copy(streamPosition = re.event.streamPosition),
              link           = re.link.copy(streamPosition = re.link.streamPosition))
    )

    test("logPosition") {
      assertEquals(er.logPosition, er.logPosition)
      assertEquals(re.logPosition, er.logPosition)
    }

    test("render") {

      assertEquals(
        er.render,
        s"""
        |EventRecord(
        |  streamId       = ${er.streamId.render},
        |  eventId        = ${er.eventData.eventId},
        |  type           = ${er.eventData.eventType.render},
        |  streamPosition = ${er.streamPosition.value.render},
        |  logPosition    = ${er.logPosition.render},
        |  data           = ${er.eventData.renderData},
        |  metadata       = ${er.eventData.renderMetadata},
        |  created        = ${er.created}
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

//======================================================================================================================

class EventTypeSuite extends SecSuite:

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

//======================================================================================================================

class EventDataSuite extends SecSuite:

  import ContentType.{Binary, Json}

  def encode(data: String): ByteVector =
    encodeToBV(data).unsafe

  val bve: ByteVector      = ByteVector.empty
  val et: EventType.Normal = EventType("eventType").unsafeGet
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
      assertEquals(EventData(et, id, data, ct), EventData(et, id, data, bve, ct))
      assertEquals(EventData(et, id, data, meta, ct), EventData(et, id, data, meta, ct))

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

//======================================================================================================================

class ContentTypeSuite extends SecSuite:

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

//======================================================================================================================
