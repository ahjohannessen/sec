package sec

import scala.concurrent.duration._
import cats.implicits._
import io.circe._
import io.circe.syntax._
import io.circe.generic.semiauto._
import org.specs2._
import core._

class MainSpec extends mutable.Specification {

  final case class Item(sku: String, origin: Option[String])
  final case class CustomMetadata(
    name: String,
    code: Int,
    items: List[Item]
  )

  implicit val codecForItems: Codec[Item]                             = deriveCodec[Item]
  implicit val codecForCustomMetadata: Codec.AsObject[CustomMetadata] = deriveCodec[CustomMetadata]

  "Meta" should {
    "be fun" >> {

      val acl      = StreamAcl.empty.copy(readRoles = Set("c1"), writeRoles = Set("c2"))
      val settings = StreamState.empty.copy(maxAge = 10.hours.some, maxCount = 50000.some, acl = acl.some)
      val custom   = CustomMetadata("test", 10, List(Item("sku1", None), Item("sku2", Some("nz"))))
      val meta     = StreamMetadata(settings, custom.asJsonObject.some)

      val enc = Encoder[StreamMetadata].apply(meta)
      val dec = Decoder[StreamMetadata].decodeJson(enc)
      val cus = dec >>= { _.custom.traverse(c => Decoder[CustomMetadata].decodeJson(c.asJson)) }

      println(Printer.spaces2.copy(dropNullValues = true).print(enc))
      println(dec.map(_.settings))
      println(cus)

      val x =
        """
          |{
          |  "$maxAge" : 36000,
          |  "$maxCount" : 50000,
          |  "$acl" : {
          |    "$r" : [
          |      "customer1", "customer2"
          |    ],
          |    "$w" : "customer1"
          |    ,
          |    "$mr" : [
          |    ],
          |    "$mw" : [
          |    ]
          |  }
          |}
          |""".stripMargin

      val js = parser.parse(x).flatMap(Decoder[StreamMetadata].decodeJson)

      println(js)

      success

    }
  }

}
