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
package api
package mapping

import cats.syntax.all._
import com.google.protobuf.ByteString
import scodec.bits.ByteVector
import sec.api.mapping.implicits._
import java.nio.charset.CharacterCodingException

class ImplicitsSuite extends SecSuite {

  test("OptionOps.require") {
    assertEquals(
      Option.empty[Int].require[ErrorOr]("test"),
      Left(ProtoResultError("Required value test missing or invalid."))
    )

    assertEquals(
      Option.empty[Int].require[ErrorOr]("test", details = "Oh Noes!".some),
      Left(ProtoResultError("Required value test missing or invalid. Oh Noes!"))
    )

    assertEquals(Option(1).require[ErrorOr]("test"), Right(1))
  }

  test("ByteVectorOps.toByteString") {
    assertEquals(
      ByteVector.encodeUtf8("abc").map(_.toByteString),
      Right(ByteString.copyFromUtf8("abc"))
    )
  }

  test("ByteStringOps.toByteVector") {
    assertEquals(
      ByteString.copyFromUtf8("abc").toByteVector.asRight[CharacterCodingException],
      ByteVector.encodeUtf8("abc")
    )
  }

}
