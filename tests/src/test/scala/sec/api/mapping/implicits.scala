/*
 * Copyright 2020 Alex Henning Johannessen
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
package api
package mapping

import cats.syntax.all._
import com.google.protobuf.ByteString
import org.specs2._
import scodec.bits.ByteVector
import sec.api.mapping.implicits._

class ImplicitsSpec extends mutable.Specification {

  "OptionOps.require" >> {
    Option.empty[Int].require[ErrorOr]("test") should beLike {
      case Left(ProtoResultError("Required value test missing or invalid.")) => ok
    }

    Option.empty[Int].require[ErrorOr]("test", details = "Oh Noes!".some) should beLike {
      case Left(ProtoResultError("Required value test missing or invalid. Oh Noes!")) => ok
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
