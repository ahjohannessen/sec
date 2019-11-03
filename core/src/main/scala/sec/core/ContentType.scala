package sec
package core

import cats.Show

sealed trait ContentType
object ContentType {

  case object Binary extends ContentType
  case object Json   extends ContentType

  implicit val showForContentType: Show[ContentType] = Show.show {
    case Binary => "ContentType.Binary"
    case Json   => "ContentType.Json"
  }

  final implicit class ContentTypeOps(val ct: ContentType) extends AnyVal {
    def fold[A](binary: => A, json: => A): A = ct match {
      case Binary => binary
      case Json   => json
    }
    def isJson: Boolean   = ct.fold(false, true)
    def isBinary: Boolean = ct.fold(true, false)
  }

}
