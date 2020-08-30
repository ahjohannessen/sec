package sec
package api
package mapping

import com.google.protobuf.ByteString
import scodec.bits.ByteVector
import org.specs2._
import cats.implicits._
import sec.api.mapping.implicits._

class ImplicitsSpec extends mutable.Specification {

  "OptionOps.require" >> {
    Option.empty[Int].require[ErrorOr]("test") should beLike {
      case Left(ProtoResultError("Required value test missing or invalid.")) => ok
    }

    Option(1).require[ErrorOr]("test") should beRight(1)
  }

  "ByteVectorOps.toByteString" >> {
    ByteVector.encodeUtf8("abc").map(_.toByteString) should beRight(ByteString.copyFromUtf8("abc"))
  }

  "ByteStringOps.toByteVector" >> {
    ByteString.copyFromUtf8("abc").toByteVector.asRight shouldEqual ByteVector.encodeUtf8("abc")
  }

}
