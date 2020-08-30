package sec
package core

import scala.concurrent.duration._
import cats.implicits._
import org.specs2.mutable.Specification
import io.circe._
import io.circe.syntax._
import Arbitraries._

//======================================================================================================================

class StreamMetadataSpec extends Specification {

  import StreamMetadataSpec._

  "codec" >> {

    val chuck   = Custom("chuck norris", List(1, 3, 3, 7))
    val members = Members(Member("jimmy banana") :: Member("joe doe") :: Nil)

    // roundtrips without custom

    val sm1 = StreamMetadata(sampleOf[StreamState], None)
    Decoder[StreamMetadata].apply(Encoder[StreamMetadata].apply(sm1).hcursor) should beRight(sm1)

    // roundtrips with custom & no overlapping keys

    val sm2 = StreamMetadata(sampleOf[StreamState], members.asJsonObject.some)
    Decoder[StreamMetadata].apply(Encoder[StreamMetadata].apply(sm2).hcursor) should beRight(sm2)

    /// roundtrips with custom & overlapping keys favors system reserved keys

    val reserved = StreamMetadata.reservedKeys

    val system = StreamState(
      maxAge         = MaxAge.from(1000.seconds).unsafe.some,
      maxCount       = None,
      cacheControl   = CacheControl.from(12.hours).unsafe.some,
      truncateBefore = EventNumber.exact(1000L).some,
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
        "members"       -> members.members.asJson
      )
    )

    val sm3      = StreamMetadata(system, custom.some)
    val encoded3 = Encoder[StreamMetadata].apply(sm3)
    val decoded3 = Decoder[StreamMetadata].apply(encoded3.hcursor)

    decoded3 should beRight(sm3.copy(custom = custom.filterKeys(k => !reserved.contains(k)).some))
    Decoder[Custom].apply(encoded3.hcursor) should beRight(chuck)
    Decoder[Members].apply(encoded3.hcursor) should beRight(members)

  }

}

object StreamMetadataSpec {

  final case class Custom(name: String, numbers: List[Int])
  object Custom {
    implicit val codecForCustom: Codec.AsObject[Custom] =
      Codec.forProduct2("name", "numbers")(Custom.apply)(c => (c.name, c.numbers))
  }

  final case class Member(name: String)
  object Member {
    implicit val codecForMember: Codec.AsObject[Member] =
      Codec.forProduct1("name")(Member.apply)(_.name)
  }

  final case class Members(members: List[Member])
  object Members {
    implicit val codecForMembers: Codec.AsObject[Members] =
      Codec.forProduct1("members")(Members.apply)(_.members)
  }

}

//======================================================================================================================

class StreamStateSpec extends Specification {

  "codec" >> {

    val ss = sampleOf[StreamState]

    val expectedMap = Map(
      "$maxAge"       -> ss.maxAge.map(_.value.toSeconds).asJson,
      "$maxCount"     -> ss.maxCount.map(_.value).asJson,
      "$tb"           -> ss.truncateBefore.map(_.value).asJson,
      "$acl"          -> ss.acl.asJson,
      "$cacheControl" -> ss.cacheControl.map(_.value.toSeconds).asJson
    )

    val expectedJson = JsonObject.fromMap(expectedMap).mapValues(_.dropNullValues).asJson

    Encoder[StreamState].apply(ss) shouldEqual expectedJson
    Decoder[StreamState].apply(expectedJson.hcursor) should beRight(ss)

  }

  "show" >> {

    StreamState.empty
      .copy(maxAge = MaxAge.from(10.days).unsafe.some, maxCount = MaxCount.from(1).unsafe.some)
      .show shouldEqual (
      s"""
       |StreamState:
       |  max-age         = 10 days
       |  max-count       = 1 event
       |  cache-control   = n/a
       |  truncate-before = n/a
       |  access-list     = n/a
       |""".stripMargin
    )

    StreamState(
      maxAge         = None,
      maxCount       = MaxCount.from(50).unsafe.some,
      cacheControl   = CacheControl.from(12.hours).unsafe.some,
      truncateBefore = EventNumber.exact(1000L).some,
      acl            = StreamAcl.empty.copy(readRoles = Set("a", "b")).some
    ).show shouldEqual (
      s"""
       |StreamState:
       |  max-age         = n/a
       |  max-count       = 50 events
       |  cache-control   = 12 hours
       |  truncate-before = EventNumber(1000)
       |  access-list     = read: [a, b], write: [], delete: [], meta-read: [], meta-write: []
       |""".stripMargin
    )

    StreamState.empty.show shouldEqual (
      s"""
       |StreamState:
       |  max-age         = n/a
       |  max-count       = n/a
       |  cache-control   = n/a
       |  truncate-before = n/a
       |  access-list     = n/a
       |""".stripMargin
    )

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

    /// Supports parsing single values / missing values

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

  "show" >> {
    StreamAcl.empty.copy(readRoles = Set("a", "b"), Set("b")).show shouldEqual (
      "read: [a, b], write: [b], delete: [], meta-read: [], meta-write: []"
    )
  }

}

//======================================================================================================================
