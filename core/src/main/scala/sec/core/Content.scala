package sec
package core

import cats.Show
import cats.implicits._
import scodec.bits.ByteVector
import scala.PartialFunction.condOpt
import Content.Type

final case class Content(
  bytes: ByteVector,
  contentType: Type
)

object Content {

  sealed trait Type
  object Type {

    case object Binary extends Type
    case object Json   extends Type

    implicit val showForContentType: Show[Type] = Show.show {
      case Binary => "Binary"
      case Json   => "Json"
    }

    final implicit class TypeOps(val tpe: Type) extends AnyVal {

      def fold[A](binary: => A, json: => A): A = tpe match {
        case Binary => binary
        case Json   => json
      }

      def isJson: Boolean   = tpe.fold(false, true)
      def isBinary: Boolean = tpe.fold(true, false)
    }

  }

  ///

  val Empty: Content = Content(ByteVector.empty, Type.Binary)

  def apply(content: String): Attempt[Content] =
    ByteVector.encodeUtf8(content).map(Content(_, Type.Binary)).leftMap(_.getMessage)

  object Json {

    def apply(content: String): Attempt[Content] =
      ByteVector.encodeUtf8(content).map(Content(_, Type.Json)).leftMap(_.getMessage)

    def unapply(content: Content): Option[String] = condOpt(content) {
      case Content(x, Type.Json) => x.decodeUtf8.getOrElse("Failed decode utf8")
    }
  }

  ///

  implicit val showForContent: Show[Content] = Show.show { c =>
    c.contentType match {
      case Type.Json => s"Json(${if (c.bytes.isEmpty) "<empty>" else c.bytes.decodeUtf8.getOrElse("Failed read json")})"
      case _         => s"Binary(${c.bytes.toString})"
    }
  }

}
