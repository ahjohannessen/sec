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

import cats.syntax.all._
import org.scalacheck._
import cats.kernel.laws.discipline._
import org.specs2.mutable.Specification
import org.typelevel.discipline.specs2.mutable.Discipline
import sec.helpers.implicits._
import sec.arbitraries._

class StreamIdSpec extends Specification with Discipline {

  import StreamId.{systemStreams => ss}

  val normalId: StreamId.Normal = StreamId.normal("normal").unsafe
  val systemId: StreamId.System = StreamId.system("system").unsafe

  "apply" >> {
    StreamId("") should beLeft(InvalidInput("name cannot be empty"))
    StreamId("$$meta") should beLeft(InvalidInput("value must not start with $$, but is $$meta"))
    StreamId("$users") shouldEqual StreamId.system("users")
    StreamId("users") shouldEqual StreamId.normal("users")
    StreamId(ss.All) shouldEqual StreamId.All.asRight
    StreamId(ss.Settings) shouldEqual StreamId.Settings.asRight
    StreamId(ss.Stats) shouldEqual StreamId.Stats.asRight
    StreamId(ss.Scavenges) shouldEqual StreamId.Scavenges.asRight
    StreamId(ss.Streams) shouldEqual StreamId.Streams.asRight
  }

  "streamIdToString" >> {
    StreamId.streamIdToString(StreamId.All) shouldEqual ss.All
    StreamId.streamIdToString(StreamId.Settings) shouldEqual ss.Settings
    StreamId.streamIdToString(StreamId.Stats) shouldEqual ss.Stats
    StreamId.streamIdToString(StreamId.Scavenges) shouldEqual ss.Scavenges
    StreamId.streamIdToString(StreamId.Streams) shouldEqual ss.Streams
    StreamId.streamIdToString(systemId) shouldEqual "$system"
    StreamId.streamIdToString(normalId) shouldEqual "normal"
    StreamId.streamIdToString(systemId.metaId) shouldEqual "$$$system"
    StreamId.streamIdToString(normalId.metaId) shouldEqual "$$normal"
  }

  "stringToStreamId" >> {
    StreamId.stringToStreamId("$$normal") shouldEqual normalId.metaId.asRight
    StreamId.stringToStreamId("$$$system") shouldEqual systemId.metaId.asRight
    StreamId.stringToStreamId(ss.All) shouldEqual StreamId.All.asRight
    StreamId.stringToStreamId(ss.Settings) shouldEqual StreamId.Settings.asRight
    StreamId.stringToStreamId(ss.Stats) shouldEqual StreamId.Stats.asRight
    StreamId.stringToStreamId(ss.Scavenges) shouldEqual StreamId.Scavenges.asRight
    StreamId.stringToStreamId(ss.Streams) shouldEqual StreamId.Streams.asRight
    StreamId.stringToStreamId(systemId.stringValue) should beRight(systemId)
    StreamId.stringToStreamId(normalId.stringValue) should beRight(normalId)
  }

  "show" >> {
    val sid = sampleOf[StreamId]
    sid.show shouldEqual sid.stringValue
  }

  "StreamIdOps" >> {

    "fold" >> {
      normalId.fold(_ => true, _ => false, _ => false) should beTrue
      systemId.fold(_ => false, _ => true, _ => false) should beTrue
      List(normalId, systemId).map(_.metaId.fold(_ => false, _ => false, _ => true)).forall(identity) should beTrue
    }

    "stringValue" >> {
      val sid = sampleOf[StreamId]
      sid.stringValue shouldEqual StreamId.streamIdToString(sid)
    }

    "isNormalStream" >> {
      normalId.isNormal should beTrue
      systemId.isNormal should beFalse
      List(normalId, systemId).map(_.metaId.isNormal).forall(identity) should beFalse
    }

    "isSystemOrMeta" >> {
      normalId.isSystemOrMeta should beFalse
      systemId.isSystemOrMeta should beTrue
      List(normalId, systemId).map(_.metaId.isSystemOrMeta).forall(identity) should beTrue
    }

  }

  "IdOps" >> {
    "meta" >> {
      val id = sampleOf[StreamId.Id]
      id.metaId shouldEqual StreamId.MetaId(id)
    }
  }

  "Eq" >> {
    implicit val cogen: Cogen[StreamId] = Cogen[String].contramap[StreamId](_.show)
    checkAll("StreamId", EqTests[StreamId].eqv)
  }

}
