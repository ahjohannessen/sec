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
import org.specs2.mutable.Specification
import scodec.bits.ByteVector
import sec.arbitraries._
import sec.helpers.implicits._
import sec.helpers.text.encodeToBV

//======================================================================================================================

class EventSpec extends Specification {

  private def bv(data: String): ByteVector =
    ByteVector.encodeUtf8(data).leftMap(_.getMessage).unsafe

  val er: EventRecord[PositionInfo.Global] = sec.EventRecord[PositionInfo.Global](
    StreamId("abc-1234").unsafe,
    PositionInfo.Global(StreamPosition.exact(5L), LogPosition.exact(42L, 42L)),
    EventData("et", sampleOf[ju.UUID], bv("abc"), ContentType.Binary).unsafe,
    sampleOf[ZonedDateTime]
  )

  val link: EventRecord[PositionInfo.Global] = sec.EventRecord[PositionInfo.Global](
    StreamId.system("ce-abc").unsafe,
    PositionInfo.Global(StreamPosition.exact(10L), LogPosition.exact(1337L, 1337L)),
    EventData(EventType.LinkTo, sampleOf[ju.UUID], bv("5@abc-1234"), ContentType.Binary),
    sampleOf[ZonedDateTime]
  )

  val re: ResolvedEvent[PositionInfo.Global] = ResolvedEvent(er, link)

  ///

  "EventOps" >> {

    "fold" >> {
      er.fold(_ => ok, _ => ko)
      re.fold(_ => ko, _ => ok)
    }

    def testCommon[P <: PositionInfo](er: EventRecord[P], re: ResolvedEvent[P]) = {

      "streamId" >> {
        (er: Event[P]).streamId shouldEqual er.streamId
        (re: Event[P]).streamId shouldEqual er.streamId
      }

      "streamPosition" >> {
        (er: Event[P]).streamPosition shouldEqual er.streamPosition
        (re: Event[P]).streamPosition shouldEqual er.streamPosition
      }

      "eventData" >> {
        (er: Event[P]).eventData shouldEqual er.eventData
        (re: Event[P]).eventData shouldEqual er.eventData
      }

      "record" >> {
        (er: Event[P]).record shouldEqual er
        (re: Event[P]).record shouldEqual re.link
      }

      "created" >> {
        (er: Event[P]).created shouldEqual er.created
        (re: Event[P]).created shouldEqual er.created
      }
    }

    testCommon(er, re)
    testCommon(
      er.copy(position = er.position.streamPosition),
      re.copy(event    = re.event.copy(position = re.event.position.streamPosition),
              link     = re.link.copy(position = re.link.position.streamPosition))
    )

    "logPosition" >> {
      (er: Event[PositionInfo.Global]).logPosition shouldEqual er.logPosition
      (re: Event[PositionInfo.Global]).logPosition shouldEqual er.logPosition
    }

  }

  "render" >> {

    er.render shouldEqual s"""
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

    re.render shouldEqual s"""
        |ResolvedEvent(
        |  event = ${re.event.render},
        |  link  = ${re.link.render}
        |)
        |""".stripMargin

  }

}

//======================================================================================================================

class EventTypeSpec extends Specification {

  val normal: EventType = EventType.Normal.unsafe("user")
  val system: EventType = EventType.System.unsafe("system")

  "apply" >> {
    EventType("") should beLeft(InvalidInput("Event type name cannot be empty"))
    EventType("$users") should beLeft(InvalidInput("value must not start with $, but is $users"))
    EventType("users") should beRight(EventType.normal("users").unsafe)
  }

  "eventTypeToString" >> {
    EventType.eventTypeToString(EventType.StreamDeleted) shouldEqual "$streamDeleted"
    EventType.eventTypeToString(EventType.StatsCollected) shouldEqual "$statsCollected"
    EventType.eventTypeToString(EventType.LinkTo) shouldEqual "$>"
    EventType.eventTypeToString(EventType.StreamReference) shouldEqual "$@"
    EventType.eventTypeToString(EventType.StreamMetadata) shouldEqual "$metadata"
    EventType.eventTypeToString(EventType.Settings) shouldEqual "$settings"
    EventType.eventTypeToString(normal) shouldEqual "user"
    EventType.eventTypeToString(system) shouldEqual s"$$system"
  }

  "stringToEventType" >> {
    EventType.stringToEventType("$streamDeleted") shouldEqual EventType.StreamDeleted.asRight
    EventType.stringToEventType("$statsCollected") shouldEqual EventType.StatsCollected.asRight
    EventType.stringToEventType("$>") shouldEqual EventType.LinkTo.asRight
    EventType.stringToEventType("$@") shouldEqual EventType.StreamReference.asRight
    EventType.stringToEventType("$metadata") shouldEqual EventType.StreamMetadata.asRight
    EventType.stringToEventType("$settings") shouldEqual EventType.Settings.asRight
    EventType.stringToEventType(normal.stringValue) should beRight(normal)
    EventType.stringToEventType(system.stringValue) should beRight(system)
  }

  "render" >> {
    val et = sampleOf[EventType]
    et.render shouldEqual et.stringValue
  }

  "EventTypeOps" >> {
    "stringValue" >> {
      val et = sampleOf[EventType]
      et.stringValue shouldEqual EventType.eventTypeToString(et)
    }
  }

}

//======================================================================================================================

class EventDataSpec extends Specification {

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

  "apply" >> {

    val errEmpty = InvalidInput("Event type name cannot be empty")
    val errStart = InvalidInput("value must not start with $, but is $system")

    def testCommon(data: ByteVector, meta: ByteVector, ct: ContentType) = {

      EventData("", id, data, ct) should beLeft(errEmpty)
      EventData("", id, data, meta, ct) should beLeft(errEmpty)
      EventData("$system", id, data, ct) should beLeft(errStart)
      EventData("$system", id, data, meta, ct) should beLeft(errStart)
      EventData(et, id, data, ct) should beLike { case EventData(`et`, `id`, `data`, `bve`, `ct`) => ok }
      EventData(et, id, data, meta, ct) should beLike { case EventData(`et`, `id`, `data`, `meta`, `ct`) => ok }
    }

    ///

    testCommon(dataJson, metaJson, Json)
    testCommon(dataBinary, metaBinary, Binary)
  }

  "EventData" >> {

    "render" >> {
      EventData.render(bve, Json) shouldEqual "Json(empty)"
      EventData.render(bve, Binary) shouldEqual "Binary(empty)"
      EventData.render(encode("""{ "link" : "1@a" }"""), Json) shouldEqual """{ "link" : "1@a" }"""
      EventData.render(encode("a"), Binary) shouldEqual "Binary(1 bytes, 0x61)"
      EventData.render(encode("a" * 31), Binary) shouldEqual s"Binary(31 bytes, 0x${"61" * 31})"
      EventData.render(encode("a" * 32), Binary) shouldEqual "Binary(32 bytes, #-547736941)"
    }

  }

}

//======================================================================================================================

class ContentTypeSpec extends Specification {

  import ContentType._

  "render" >> {
    (Binary: ContentType).render shouldEqual "Binary"
    (Json: ContentType).render shouldEqual "Json"
  }

  "ContentTypeOps" >> {

    "fold" >> {
      Binary.fold("b", "j") shouldEqual "b"
      Json.fold("b", "j") shouldEqual "j"
    }

    "isJson" >> {
      Json.isJson should beTrue
      Binary.isJson should beFalse
    }

    "isBinary" >> {
      Binary.isBinary should beTrue
      Json.isBinary should beFalse
    }

  }

}

//======================================================================================================================
