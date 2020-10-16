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

import java.{util => ju}
import java.time.ZonedDateTime
import java.util.UUID

import cats.syntax.all._
import scodec.bits.ByteVector
import org.specs2.mutable.Specification
import sec.arbitraries._
import sec.helpers.implicits._
import sec.helpers.text.encodeToBV

//======================================================================================================================

class EventSpec extends Specification {

  private def bv(data: String): ByteVector =
    ByteVector.encodeUtf8(data).leftMap(_.getMessage).unsafe

  val er: EventRecord = sec.EventRecord(
    StreamId("abc-1234").unsafe,
    StreamPosition.exact(5L),
    sampleOf[LogPosition.Exact],
    EventData("et", sampleOf[ju.UUID], bv("abc"), ContentType.Binary).unsafe,
    sampleOf[ZonedDateTime]
  )

  val link: EventRecord = sec.EventRecord(
    StreamId.system("ce-abc").unsafe,
    StreamPosition.exact(10L),
    LogPosition.exact(1337L, 1337L),
    EventData(EventType.LinkTo, sampleOf[ju.UUID], bv("5@abc-1234"), ContentType.Binary),
    sampleOf[ZonedDateTime]
  )

  val re: ResolvedEvent = ResolvedEvent(er, link)

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

    "streamPosition" >> {
      (er: Event).streamPosition shouldEqual er.streamPosition
      (re: Event).streamPosition shouldEqual er.streamPosition
    }

    "logPosition" >> {
      (er: Event).logPosition shouldEqual er.logPosition
      (re: Event).logPosition shouldEqual er.logPosition
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

    er.show shouldEqual s"""
        |EventRecord(
        |  streamId       = ${er.streamId.show},
        |  eventId        = ${er.eventData.eventId},
        |  type           = ${er.eventData.eventType.show},
        |  streamPosition = ${er.streamPosition.show},
        |  logPosition    = ${er.logPosition.show},
        |  data           = ${er.eventData.showData}, 
        |  metadata       = ${er.eventData.showMetadata}, 
        |  created        = ${er.created}
        |)
        |""".stripMargin

    re.show shouldEqual s"""
        |ResolvedEvent(
        |  event = ${re.event.show},
        |  link  = ${re.link.show}
        |)
        |""".stripMargin

  }

}

//======================================================================================================================

class EventTypeSpec extends Specification {

  import EventType.{systemTypes => st}

  val usr: EventType = EventType.userDefined("user").unsafe
  val sys: EventType = EventType.systemDefined("system").unsafe

  "apply" >> {
    EventType("") should beLeft(InvalidInput("Event type name cannot be empty"))
    EventType("$users") should beLeft(InvalidInput("value must not start with $, but is $users"))
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

  import ContentType.{Binary, Json}

  def encode(data: String): ByteVector =
    encodeToBV(data).unsafe

  val bve: ByteVector           = ByteVector.empty
  val et: EventType.UserDefined = EventType("eventType").unsafe
  val id: UUID                  = sampleOf[ju.UUID]

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

  "show" >> {
    (Binary: ContentType).show shouldEqual "Binary"
    (Json: ContentType).show shouldEqual "Json"
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
