package sec
package core

import cats.Show
import cats.implicits._
import scodec.bits.ByteVector
import scala.PartialFunction.condOpt

final case class Content(
  data: ByteVector,
  contentType: ContentType
)

object Content {

  val Empty: Content = Content(ByteVector.empty, ContentType.Binary)

  def apply(content: String): Attempt[Content] =
    ByteVector.encodeUtf8(content).map(Content(_, ContentType.Binary)).leftMap(_.getMessage)

  object Json {

    def apply(content: String): Attempt[Content] =
      ByteVector.encodeUtf8(content).map(Content(_, ContentType.Json)).leftMap(_.getMessage)

    def unapply(content: Content): Option[String] = condOpt(content) {
      case Content(x, ContentType.Json) => x.decodeUtf8.getOrElse("Failed decode utf8")
    }
  }

  ///

  implicit val showForContent: Show[Content] = Show.show { c =>
    val data = c.contentType match {
      case ContentType.Json if c.data.nonEmpty => c.data.decodeUtf8.getOrElse("Failed read json")
      case _                                   => c.data.toString
    }
    s"Content($data, ${c.contentType})"
  }

}
