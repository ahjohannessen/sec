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

import scala.concurrent.duration._

import cats.syntax.all._
import io.circe._
import io.circe.syntax._
import org.specs2.mutable.Specification
import sec.arbitraries._
import sec.helpers.implicits._

//======================================================================================================================

class StreamMetadataSpec extends Specification {

  "codec" >> {

    val chuck = Custom("chuck norris", List(1, 3, 3, 7))
    val foo   = Foo(Bar("jimmy banana") :: Bar("joe doe") :: Nil)

    // roundtrips without custom

    val sm1 = StreamMetadata(sampleOf[MetaState], None)
    Decoder[StreamMetadata].apply(Encoder[StreamMetadata].apply(sm1).hcursor) should beRight(sm1)

    // roundtrips with custom & no overlapping keys

    val sm2 = StreamMetadata(sampleOf[MetaState], foo.asJsonObject.some)
    Decoder[StreamMetadata].apply(Encoder[StreamMetadata].apply(sm2).hcursor) should beRight(sm2)

    // / roundtrips with custom & overlapping keys favors system reserved keys

    val reserved = StreamMetadata.reservedKeys

    val system = MetaState(
      maxAge         = MaxAge(1000.seconds).unsafe.some,
      maxCount       = None,
      cacheControl   = CacheControl(12.hours).unsafe.some,
      truncateBefore = StreamPosition.exact(1000L).some,
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

    decoded3 should beRight(sm3.copy(custom = custom.filterKeys(k => !reserved.contains(k)).some))
    Decoder[Custom].apply(encoded3.hcursor) should beRight(chuck)
    Decoder[Foo].apply(encoded3.hcursor) should beRight(foo)

  }

}

//======================================================================================================================

class MetaStateSpec extends Specification {

  "codec" >> {

    val ms = sampleOf[MetaState]

    val expectedMap = Map(
      "$maxAge"       -> ms.maxAge.map(_.value.toSeconds).asJson,
      "$maxCount"     -> ms.maxCount.map(_.value).asJson,
      "$tb"           -> ms.truncateBefore.map(_.value).asJson,
      "$acl"          -> ms.acl.asJson,
      "$cacheControl" -> ms.cacheControl.map(_.value.toSeconds).asJson
    )

    val expectedJson = JsonObject.fromMap(expectedMap).mapValues(_.dropNullValues).asJson

    Encoder[MetaState].apply(ms) shouldEqual expectedJson
    Decoder[MetaState].apply(expectedJson.hcursor) should beRight(ms)

  }

  "render" >> {

    MetaState.empty
      .copy(maxAge = MaxAge(10.days).toOption, maxCount = MaxCount(1).toOption)
      .render shouldEqual s"""
       |MetaState:
       |  max-age         = 10 days
       |  max-count       = 1 event
       |  cache-control   = n/a
       |  truncate-before = n/a
       |  access-list     = n/a
       |""".stripMargin

    MetaState(
      maxAge         = None,
      maxCount       = MaxCount(50).toOption,
      cacheControl   = CacheControl(12.hours).toOption,
      truncateBefore = StreamPosition.exact(1000L).some,
      acl            = StreamAcl.empty.copy(readRoles = Set("a", "b")).some
    ).render shouldEqual s"""
       |MetaState:
       |  max-age         = n/a
       |  max-count       = 50 events
       |  cache-control   = 12 hours
       |  truncate-before = 1000L
       |  access-list     = read: [a, b], write: [], delete: [], meta-read: [], meta-write: []
       |""".stripMargin

    MetaState.empty.render shouldEqual s"""
       |MetaState:
       |  max-age         = n/a
       |  max-count       = n/a
       |  cache-control   = n/a
       |  truncate-before = n/a
       |  access-list     = n/a
       |""".stripMargin

  }

}

//======================================================================================================================

class StreamAclSpec extends Specification {

  "codec" >> {

    val acl = sampleOf[StreamAcl]

    val expectedMap = Map(
      "$r"  -> acl.readRoles,
      "$w"  -> acl.writeRoles,
      "$d"  -> acl.deleteRoles,
      "$mr" -> acl.metaReadRoles,
      "$mw" -> acl.metaWriteRoles
    ).filter(_._2.nonEmpty).view.mapValues(_.asJson).toMap

    val expectedJson = JsonObject.fromMap(expectedMap).asJson

    Encoder[StreamAcl].apply(acl) shouldEqual expectedJson
    Decoder[StreamAcl].apply(expectedJson.hcursor) should beRight(acl)

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

    parser.parse(aclJson).flatMap(Decoder[StreamAcl].decodeJson) should beRight(
      StreamAcl.empty.copy(readRoles = Set("a", "b"), writeRoles = Set("b"))
    )

  }

  "render" >> {
    StreamAcl.empty
      .copy(readRoles = Set("a", "b"), Set("b"))
      .render shouldEqual "read: [a, b], write: [b], delete: [], meta-read: [], meta-write: []"
  }

}

//======================================================================================================================
