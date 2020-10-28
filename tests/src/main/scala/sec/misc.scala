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

import io.circe.Codec

//======================================================================================================================

final case class Custom(name: String, numbers: List[Int])
object Custom {
  implicit val codecForCustom: Codec.AsObject[Custom] =
    Codec.forProduct2("name", "numbers")(Custom.apply)(c => (c.name, c.numbers))
}

final case class Bar(name: String)
object Bar {
  implicit val codecForBar: Codec.AsObject[Bar] =
    Codec.forProduct1("name")(Bar.apply)(_.name)
}

final case class Foo(bars: List[Bar])
object Foo {
  implicit val codecForFoo: Codec.AsObject[Foo] =
    Codec.forProduct1("bars")(Foo.apply)(_.bars)
}

//======================================================================================================================
