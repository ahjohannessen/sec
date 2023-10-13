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

import scala.util.matching.Regex
import cats.data.NonEmptyList
import cats.syntax.all.*
import org.scalacheck.*
import org.scalacheck.Prop.forAll

class EventFilterSuite extends SecDisciplineSuite:

  import EventFilter._

  group("EventFilter") {

    implicit val arbKind: Arbitrary[Kind]   = Arbitrary(Gen.oneOf[Kind](ByStreamId, ByEventType))
    implicit val arbRegex: Arbitrary[Regex] = Arbitrary(Gen.oneOf("^ctx1__.*".r, "^[^$].*".r))

    property("prefix") {
      forAll { (k: Kind, fst: String, rest: List[String]) =>
        assertEquals(
          prefix(k, fst, rest: _*),
          EventFilter(k, NonEmptyList(PrefixFilter(fst), rest.map(x => PrefixFilter(x))).asLeft)
        )
      }
    }

    property("regex") {
      forAll { (k: Kind, filter: Regex) =>
        assertEquals(regex(k, filter.pattern.toString), EventFilter(k, RegexFilter(filter).asRight))
      }
    }

  }
