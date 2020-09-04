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
package core

import java.{util => ju}
import java.time.ZonedDateTime
import cats.syntax.all._
import scodec.bits.ByteVector
import org.specs2.mutable.Specification
import Arbitraries._

//======================================================================================================================

class EventSpec extends Specification {

  val er = EventRecord(
    StreamId.from("abc-1234").unsafe,
    EventNumber.exact(5L),
    sampleOf[Position.Exact],
    EventData("et", sampleOf[ju.UUID], Content.binary("abc").unsafe).unsafe,
    sampleOf[ZonedDateTime]
  )

  val link = EventRecord(
    StreamId.system("ce-abc").unsafe,
    EventNumber.exact(10L),
    Position.exact(1337L, 1337L),
    EventData(EventType.LinkTo, sampleOf[ju.UUID], Content.binary("5@abc-1234").unsafe),
    sampleOf[ZonedDateTime]
  )

  val re = ResolvedEvent(er, link)

  ///

  "EventOps" >> {

    "fold" >> {
      er.fold(_ => ok, _ => ko)
      re.fold(_ => ko, _ => ok)
    }

    "streamId" >> {
      (er: Event).streamId shouldEqual er.streamId
      (re: Event).streamId shouldEqual er.streamId
    }

    "number" >> {
      (er: Event).number shouldEqual er.number
      (re: Event).number shouldEqual er.number
    }

    "position" >> {
      (er: Event).position shouldEqual er.position
      (re: Event).position shouldEqual er.position
    }

    "eventData" >> {
      (er: Event).eventData shouldEqual er.eventData
      (re: Event).eventData shouldEqual er.eventData
    }

    "record" >> {
      (er: Event).record shouldEqual er
      (re: Event).record shouldEqual link
    }

    "created" >> {
      (er: Event).created shouldEqual er.created
      (re: Event).created shouldEqual er.created
    }

  }

  "show" >> {

    er.show shouldEqual (
      s"""
        |EventRecord(
        |  streamId = ${er.streamId.show},
        |  eventId  = ${er.eventData.eventId},
        |  type     = ${er.eventData.eventType.show},
        |  number   = ${er.number.show},
        |  position = ${er.position.show},
        |  data     = ${er.eventData.data.show}, 
        |  metadata = ${er.eventData.metadata.show}, 
        |  created  = ${er.created}
        |)
        |""".stripMargin
    )

    re.show shouldEqual (
      s"""
        |ResolvedEvent(
        |  event = ${re.event.show},
        |  link  = ${re.link.show}
        |)
        |""".stripMargin
    )

  }

}

//======================================================================================================================

class EventTypeSpec extends Specification {

  import EventType.{systemTypes => st}

  val usr: EventType = EventType.userDefined("user").unsafe
  val sys: EventType = EventType.systemDefined("system").unsafe

  "apply" >> {
    EventType("") should beLeft("Event type name cannot be empty")
    EventType("$users") should beLeft("value must not start with $, but is $users")
    EventType("users") should beRight(EventType.userDefined("users").unsafe)
  }

  "eventTypeToString" >> {
    EventType.eventTypeToString(EventType.StreamDeleted) shouldEqual st.StreamDeleted
    EventType.eventTypeToString(EventType.StatsCollected) shouldEqual st.StatsCollected
    EventType.eventTypeToString(EventType.LinkTo) shouldEqual st.LinkTo
    EventType.eventTypeToString(EventType.StreamReference) shouldEqual st.StreamReference
    EventType.eventTypeToString(EventType.StreamMetadata) shouldEqual st.StreamMetadata
    EventType.eventTypeToString(EventType.Settings) shouldEqual st.Settings
    EventType.eventTypeToString(usr) shouldEqual "user"
    EventType.eventTypeToString(sys) shouldEqual "$system"
  }

  "stringToEventType" >> {
    EventType.stringToEventType(st.StreamDeleted) shouldEqual EventType.StreamDeleted.asRight
    EventType.stringToEventType(st.StatsCollected) shouldEqual EventType.StatsCollected.asRight
    EventType.stringToEventType(st.LinkTo) shouldEqual EventType.LinkTo.asRight
    EventType.stringToEventType(st.StreamReference) shouldEqual EventType.StreamReference.asRight
    EventType.stringToEventType(st.StreamMetadata) shouldEqual EventType.StreamMetadata.asRight
    EventType.stringToEventType(st.Settings) shouldEqual EventType.Settings.asRight
    EventType.stringToEventType(EventType.eventTypeToString(usr)) should beRight(usr)
    EventType.stringToEventType(EventType.eventTypeToString(sys)) should beRight(sys)
  }

  "show" >> {
    val et = sampleOf[EventType]
    et.show shouldEqual EventType.eventTypeToString(et)
  }

}

//======================================================================================================================

class EventDataSpec extends Specification {

  import Content.Type.{Binary, Json}

  val bve = ByteVector.empty
  val et  = EventType("eventType").unsafe
  val id  = sampleOf[ju.UUID]

  val dataJson   = Content.json("""{ "data": "1" }""").unsafe
  val metaJson   = Content.json("""{ "meta": "2" }""").unsafe
  val dataBinary = Content.binary("data").unsafe
  val metaBinary = Content.binary("meta").unsafe

  "apply" >> {

    val errEmpty   = "Event type name cannot be empty"
    val errStart   = "value must not start with $, but is $system"
    val errContent = "Different content types for data & metadata is not supported."

    def testCommon(data: Content, meta: Content) = {

      val empty = Content.empty(data.contentType)

      EventData("", id, data) should beLeft(errEmpty)
      EventData("", id, data, meta) should beLeft(errEmpty)
      EventData("$system", id, data) should beLeft(errStart)
      EventData("$system", id, data, meta) should beLeft(errStart)
      EventData(et, id, data) should beLike { case EventData(`et`, `id`, `data`, `empty`) => ok }
      EventData(et, id, data, meta) should beLike { case Right(EventData(`et`, `id`, `data`, `meta`)) => ok }
    }

    ///

    testCommon(dataJson, metaJson)
    testCommon(dataBinary, metaBinary)

    EventData(et, id, dataBinary, metaJson) should beLeft(errContent)
    EventData(et, id, dataJson, metaBinary) should beLeft(errContent)

    val json = EventData.json(et, id, bve, bve)
    json.data.contentType shouldEqual Json
    json.metadata.contentType shouldEqual Json

    val binary = EventData.binary(et, id, bve, bve)
    binary.data.contentType shouldEqual Binary
    binary.metadata.contentType shouldEqual Binary
  }

  "EventDataOps" >> {
    "contentType" >> {
      EventData(et, id, dataJson).contentType shouldEqual Content.Type.Json
      EventData(et, id, dataBinary).contentType shouldEqual Content.Type.Binary
    }
  }

}

//======================================================================================================================

class ContentSpec extends Specification {

  import Content.Type.{Binary, Json}

  "apply" >> {
    Content("\uD800", Binary) should beLeft("Input length = 1")
    Content("\uD800", Json) should beLeft("Input length = 1")
    Content("", Binary) should beRight(Content.BinaryEmpty)
    Content("", Json) should beRight(Content.JsonEmpty)
  }

  "binary" >> {
    Content.binary("1@a") shouldEqual Content("1@a", Binary)
  }

  "json" >> {
    Content.json("""{ "link" : "1@a" }""") shouldEqual Content("""{ "link" : "1@a" }""", Json)
  }

  "show" >> {
    Content.JsonEmpty.show shouldEqual ("Json(empty)")
    Content.BinaryEmpty.show shouldEqual ("Binary(empty)")
    Content.json("""{ "link" : "1@a" }""").unsafe.show shouldEqual """{ "link" : "1@a" }"""
    Content.binary("a").unsafe.show shouldEqual "Binary(1 bytes, 0x61)"
    Content.binary("a" * 31).unsafe.show shouldEqual (s"Binary(31 bytes, 0x${"61" * 31})")
    Content.binary("a" * 32).unsafe.show shouldEqual "Binary(32 bytes, #-547736941)"
  }

  "Type" >> {
    "show" >> {
      (Binary: Content.Type).show shouldEqual "Binary"
      (Json: Content.Type).show shouldEqual "Json"
    }
  }

  "TypeOps" >> {
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
