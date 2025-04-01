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

import cats.kernel.laws.discipline.*
import cats.syntax.all.*
import org.scalacheck.*
import sec.arbitraries.{*, given}

class StreamIdSuite extends SecDisciplineSuite:

  val normal: StreamId.Normal = StreamId.Normal.unsafe("normal")
  val system: StreamId.System = StreamId.System.unsafe("system")

  test("apply") {
    assertEquals(StreamId(""), Left(InvalidInput("name cannot be empty")))
    assertEquals(StreamId("$$meta"), Left(InvalidInput("value must not start with $$, but is $$meta")))
    assertEquals(StreamId("$users").leftMap(_.msg), StreamId.system("users"))
    assertEquals(StreamId("users").leftMap(_.msg), StreamId.normal("users"))
    assertEquals(StreamId("$all"), StreamId.All.asRight)
    assertEquals(StreamId("$settings"), StreamId.Settings.asRight)
    assertEquals(StreamId("$stats"), StreamId.Stats.asRight)
    assertEquals(StreamId("$scavenges"), StreamId.Scavenges.asRight)
    assertEquals(StreamId("$streams"), StreamId.Streams.asRight)
  }

  test("streamIdToString") {
    assertEquals(StreamId.streamIdToString(StreamId.All), "$all")
    assertEquals(StreamId.streamIdToString(StreamId.Settings), "$settings")
    assertEquals(StreamId.streamIdToString(StreamId.Stats), "$stats")
    assertEquals(StreamId.streamIdToString(StreamId.Scavenges), "$scavenges")
    assertEquals(StreamId.streamIdToString(StreamId.Streams), "$streams")
    assertEquals(StreamId.streamIdToString(system), s"$$system")
    assertEquals(StreamId.streamIdToString(normal), "normal")
    assertEquals(StreamId.streamIdToString(system.metaId), "$$$system")
    assertEquals(StreamId.streamIdToString(normal.metaId), "$$normal")
  }

  test("stringToStreamId") {
    assertEquals(StreamId.stringToStreamId("$$normal"), normal.metaId.asRight)
    assertEquals(StreamId.stringToStreamId("$$$system"), system.metaId.asRight)
    assertEquals(StreamId.stringToStreamId("$all"), StreamId.All.asRight)
    assertEquals(StreamId.stringToStreamId("$settings"), StreamId.Settings.asRight)
    assertEquals(StreamId.stringToStreamId("$stats"), StreamId.Stats.asRight)
    assertEquals(StreamId.stringToStreamId("$scavenges"), StreamId.Scavenges.asRight)
    assertEquals(StreamId.stringToStreamId("$streams"), StreamId.Streams.asRight)
    assertEquals(StreamId.stringToStreamId(system.stringValue), Right(system))
    assertEquals(StreamId.stringToStreamId(normal.stringValue), Right(normal))
  }

  test("render") {
    val sid = sampleOf[StreamId]
    assertEquals(sid.render, sid.stringValue)
  }

  group("StreamIdOps") {

    test("fold") {
      assert(normal.fold(_ => true, _ => false, _ => false))
      assert(system.fold(_ => false, _ => true, _ => false))
      assert(List(normal, system).map(_.metaId.fold(_ => false, _ => false, _ => true)).forall(identity))
    }

    test("stringValue") {
      val sid = sampleOf[StreamId]
      assertEquals(sid.stringValue, StreamId.streamIdToString(sid))
    }

    test("isNormalStream") {
      assert(normal.isNormal)
      assertNot(system.isNormal)
      assertNot(List(normal, system).map(_.metaId.isNormal).forall(identity))
    }

    test("isSystemOrMeta") {
      assertNot(normal.isSystemOrMeta)
      assert(system.isSystemOrMeta)
      assert(List(normal, system).map(_.metaId.isSystemOrMeta).forall(identity))
    }

  }

  group("IdOps") {

    test("meta") {
      val id = sampleOf[StreamId.Id]
      assertEquals(id.metaId, StreamId.MetaId(id))
    }
  }

  test("Eq") {
    implicit val cogen: Cogen[StreamId] = Cogen[String].contramap[StreamId](_.stringValue)
    checkAll("StreamId", EqTests[StreamId].eqv)
  }
