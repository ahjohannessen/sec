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

import scala.util.matching.Regex
import cats.data.NonEmptyList
import cats.implicits._
import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import EventFilter._

class EventFilterSpec extends Specification with ScalaCheck {

  "EventFilter" >> {

    implicit val arbKind: Arbitrary[Kind]   = Arbitrary(Gen.oneOf[Kind](ByStreamId, ByEventType))
    implicit val arbMax: Arbitrary[Int]     = Arbitrary(Gen.posNum[Int])
    implicit val arbRegex: Arbitrary[Regex] = Arbitrary(Gen.oneOf("^ctx1__.*".r, "^[^$].*".r))

    "prefix" >> prop { (k: Kind, msw: Option[Int], fst: String, rest: List[String]) =>
      prefix(k, msw, fst, rest: _*) shouldEqual
        EventFilter(k, msw, NonEmptyList(PrefixFilter(fst), rest.map(PrefixFilter)).asLeft)
    }

    "regex" >> prop { (k: Kind, msw: Option[Int], filter: Regex) =>
      regex(k, msw, filter.pattern.toString) shouldEqual EventFilter(k, msw, RegexFilter(filter).asRight)
    }

  }
}
