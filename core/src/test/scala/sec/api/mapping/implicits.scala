package sec
package api
package mapping

import java.nio.charset.CharacterCodingException
import com.google.protobuf.ByteString
import scodec.bits.ByteVector
import org.specs2._
import cats.implicits._
import sec.api.mapping.implicits._

class ImplicitsSpec extends mutable.Specification {

  "OptionOps.require" >> {
    Option.empty[Int].require[Either[Throwable, *]]("test") should beLeft(
      ProtoResultError("Required value test missing or invalid.")
    )

    Option(1).require[Either[Throwable, *]]("test") should beRight(1)
  }

  "AttemptOps.unsafe" >> {
    "oops".asLeft[Int].unsafe should throwA[IllegalArgumentException]("oops")

    1.asRight[String].unsafe shouldEqual 1
  }

  "ByteVectorOps.toByteString" >> {
    ByteVector.encodeUtf8("abc").map(_.toByteString) should beRight(ByteString.copyFromUtf8("abc"))
  }

  "ByteStringOps.toByteVector" >> {
    ByteString.copyFromUtf8("abc").toByteVector.asRight shouldEqual ByteVector.encodeUtf8("abc")
  }

}
