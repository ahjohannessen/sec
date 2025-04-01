/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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

import scala.concurrent.duration.*
import sec.utilities.*

class UtilitiesSuite extends SecSuite:

  group("guardNonEmpty") {
    assertEquals(guardNonEmpty("x")(null), Left("x cannot be empty"))
    assertEquals(guardNonEmpty("x")(""), Left("x cannot be empty"))
    assertEquals(guardNonEmpty("x")("wohoo"), Right("wohoo"))
  }

  group("guardNotStartsWith") {
    assertEquals(guardNotStartsWith("$")("$"), Left("value must not start with $, but is $"))
    assertEquals(guardNotStartsWith("$")("a$"), Right("a$"))
  }

  group("format duration") {
    assertEquals(format(150.millis), "150.0ms")
    assertEquals(format(1001.millis), "1.001s")
    assertEquals(format(4831.millis), "4.831s")
  }
