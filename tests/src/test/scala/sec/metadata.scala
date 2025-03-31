/*
 * Copyright 2020 Scala Event Sourcing client for KurrentDB
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

import scala.concurrent.duration.*
import cats.syntax.all.*
import io.circe.*
import io.circe.syntax.*
import sec.arbitraries.{*, given}
import sec.helpers.implicits.*

//======================================================================================================================

class StreamMetadataSuite extends SecSuite:

  test("codec") {

    val chuck = Custom("chuck norris", List(1, 3, 3, 7))
    val foo   = Foo(Bar("jimmy banana") :: Bar("joe doe") :: Nil)

    // roundtrips without custom

    val sm1 = StreamMetadata(sampleOf[MetaState], None)
    assertEquals(Decoder[StreamMetadata].apply(Encoder[StreamMetadata].apply(sm1).hcursor), Right(sm1))

    // roundtrips with custom & no overlapping keys

    val sm2 = StreamMetadata(sampleOf[MetaState], foo.asJsonObject.some)
    assertEquals(Decoder[StreamMetadata].apply(Encoder[StreamMetadata].apply(sm2).hcursor), Right(sm2))

    // / roundtrips with custom & overlapping keys favors system reserved keys

    val reserved = StreamMetadata.reservedKeys

    val system = MetaState(
      maxAge         = MaxAge(1000.seconds).unsafeGet.some,
      maxCount       = None,
      cacheControl   = CacheControl(12.hours).unsafeGet.some,
      truncateBefore = StreamPosition(1000L).some,
      acl            = StreamAcl.empty.copy(readRoles = Set("a", "b")).some
    )

    val custom = JsonObject.fromMap(
      Map(
        "$maxAge"       -> 2000.asJson,
        "$maxCount"     -> 500.asJson,
        "$tb"           -> 2000L.asJson,
        "$acl"          -> Json.Null,
        "$cacheControl" -> Json.Null,
        "name"          -> chuck.name.asJson,
        "numbers"       -> chuck.numbers.asJson,
        "bars"          -> foo.bars.asJson
      )
    )

    val sm3      = StreamMetadata(system, custom.some)
    val encoded3 = Encoder[StreamMetadata].apply(sm3)
    val decoded3 = Decoder[StreamMetadata].apply(encoded3.hcursor)

    assertEquals(decoded3, Right(sm3.copy(custom = custom.filterKeys(k => !reserved.contains(k)).some)))
    assertEquals(Decoder[Custom].apply(encoded3.hcursor), Right(chuck))
    assertEquals(Decoder[Foo].apply(encoded3.hcursor), Right(foo))

  }

//======================================================================================================================

class MetaStateSuite extends SecSuite:

  test("codec") {

    val ms = sampleOf[MetaState]

    val expectedMap = Map(
      "$maxAge"       -> ms.maxAge.map(_.value.toSeconds).asJson,
      "$maxCount"     -> ms.maxCount.map(_.value).asJson,
      "$tb"           -> ms.truncateBefore.map(_.value.toLong).asJson,
      "$acl"          -> ms.acl.asJson,
      "$cacheControl" -> ms.cacheControl.map(_.value.toSeconds).asJson
    )

    val expectedJson = JsonObject.fromMap(expectedMap).mapValues(_.dropNullValues).asJson

    assertEquals(Encoder[MetaState].apply(ms), expectedJson)
    assertEquals(Decoder[MetaState].apply(expectedJson.hcursor), Right(ms))
  }

  test("render") {

    assertEquals(
      MetaState.empty
        .copy(maxAge = MaxAge(10.days).toOption, maxCount = MaxCount(1).toOption)
        .render,
      s"""
       |MetaState:
       |  max-age         = 10 days
       |  max-count       = 1 event
       |  cache-control   = n/a
       |  truncate-before = n/a
       |  access-list     = n/a
       |""".stripMargin
    )

    assertEquals(
      MetaState(
        maxAge         = None,
        maxCount       = MaxCount(50).toOption,
        cacheControl   = CacheControl(12.hours).toOption,
        truncateBefore = StreamPosition(1000L).some,
        acl            = StreamAcl.empty.copy(readRoles = Set("a", "b")).some
      ).render,
      s"""
       |MetaState:
       |  max-age         = n/a
       |  max-count       = 50 events
       |  cache-control   = 12 hours
       |  truncate-before = 1000
       |  access-list     = read: [a, b], write: [], delete: [], meta-read: [], meta-write: []
       |""".stripMargin
    )

    assertEquals(
      MetaState.empty.render,
      s"""
       |MetaState:
       |  max-age         = n/a
       |  max-count       = n/a
       |  cache-control   = n/a
       |  truncate-before = n/a
       |  access-list     = n/a
       |""".stripMargin
    )

  }

//======================================================================================================================

class StreamAclSuite extends SecSuite:

  test("codec") {

    val acl = sampleOf[StreamAcl]

    val expectedMap = Map(
      "$r"  -> acl.readRoles,
      "$w"  -> acl.writeRoles,
      "$d"  -> acl.deleteRoles,
      "$mr" -> acl.metaReadRoles,
      "$mw" -> acl.metaWriteRoles
    ).filter(_._2.nonEmpty).view.mapValues(_.asJson).toMap

    val expectedJson = JsonObject.fromMap(expectedMap).asJson

    assertEquals(Encoder[StreamAcl].apply(acl), expectedJson)
    assertEquals(Decoder[StreamAcl].apply(expectedJson.hcursor), Right(acl))

    // / Supports parsing single values / missing values

    val aclJson = """
          |  {
          |    "$r" : [
          |      "a", "b"
          |    ],
          |    "$w" : "b",
          |    "$mr" : null,
          |    "$mw" : []
          |  }    
    """.stripMargin

    assertEquals(
      parser.parse(aclJson).flatMap(Decoder[StreamAcl].decodeJson),
      Right(StreamAcl.empty.copy(readRoles = Set("a", "b"), writeRoles = Set("b")))
    )

  }

  test("render") {
    assertEquals(
      StreamAcl.empty
        .copy(readRoles = Set("a", "b"), Set("b"))
        .render,
      "read: [a, b], write: [b], delete: [], meta-read: [], meta-write: []"
    )
  }

//======================================================================================================================
