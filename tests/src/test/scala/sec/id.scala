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

import cats.kernel.laws.discipline._
import cats.syntax.all._
import org.scalacheck._
import org.specs2.mutable.Specification
import org.typelevel.discipline.specs2.mutable.Discipline
import sec.arbitraries._

class StreamIdSpec extends Specification with Discipline {

  val normal: StreamId.Normal = StreamId.Normal.unsafe("normal")
  val system: StreamId.System = StreamId.System.unsafe("system")

  "apply" >> {
    StreamId("") should beLeft(InvalidInput("name cannot be empty"))
    StreamId("$$meta") should beLeft(InvalidInput("value must not start with $$, but is $$meta"))
    StreamId("$users") shouldEqual StreamId.system("users")
    StreamId("users") shouldEqual StreamId.normal("users")
    StreamId("$all") shouldEqual StreamId.All.asRight
    StreamId("$settings") shouldEqual StreamId.Settings.asRight
    StreamId("$stats") shouldEqual StreamId.Stats.asRight
    StreamId("$scavenges") shouldEqual StreamId.Scavenges.asRight
    StreamId("$streams") shouldEqual StreamId.Streams.asRight
  }

  "streamIdToString" >> {
    StreamId.streamIdToString(StreamId.All) shouldEqual "$all"
    StreamId.streamIdToString(StreamId.Settings) shouldEqual "$settings"
    StreamId.streamIdToString(StreamId.Stats) shouldEqual "$stats"
    StreamId.streamIdToString(StreamId.Scavenges) shouldEqual "$scavenges"
    StreamId.streamIdToString(StreamId.Streams) shouldEqual "$streams"
    StreamId.streamIdToString(system) shouldEqual s"$$system"
    StreamId.streamIdToString(normal) shouldEqual "normal"
    StreamId.streamIdToString(system.metaId) shouldEqual "$$$system"
    StreamId.streamIdToString(normal.metaId) shouldEqual "$$normal"
  }

  "stringToStreamId" >> {
    StreamId.stringToStreamId("$$normal") shouldEqual normal.metaId.asRight
    StreamId.stringToStreamId("$$$system") shouldEqual system.metaId.asRight
    StreamId.stringToStreamId("$all") shouldEqual StreamId.All.asRight
    StreamId.stringToStreamId("$settings") shouldEqual StreamId.Settings.asRight
    StreamId.stringToStreamId("$stats") shouldEqual StreamId.Stats.asRight
    StreamId.stringToStreamId("$scavenges") shouldEqual StreamId.Scavenges.asRight
    StreamId.stringToStreamId("$streams") shouldEqual StreamId.Streams.asRight
    StreamId.stringToStreamId(system.stringValue) should beRight(system)
    StreamId.stringToStreamId(normal.stringValue) should beRight(normal)
  }

  "show" >> {
    val sid = sampleOf[StreamId]
    sid.show shouldEqual sid.stringValue
  }

  "StreamIdOps" >> {

    "fold" >> {
      normal.fold(_ => true, _ => false, _ => false) should beTrue
      system.fold(_ => false, _ => true, _ => false) should beTrue
      List(normal, system).map(_.metaId.fold(_ => false, _ => false, _ => true)).forall(identity) should beTrue
    }

    "stringValue" >> {
      val sid = sampleOf[StreamId]
      sid.stringValue shouldEqual StreamId.streamIdToString(sid)
    }

    "isNormalStream" >> {
      normal.isNormal should beTrue
      system.isNormal should beFalse
      List(normal, system).map(_.metaId.isNormal).forall(identity) should beFalse
    }

    "isSystemOrMeta" >> {
      normal.isSystemOrMeta should beFalse
      system.isSystemOrMeta should beTrue
      List(normal, system).map(_.metaId.isSystemOrMeta).forall(identity) should beTrue
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
