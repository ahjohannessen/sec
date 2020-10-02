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

import cats.kernel.laws.discipline._
import org.scalacheck._
import org.specs2.mutable.Specification
import org.typelevel.discipline.specs2.mutable.Discipline
import sec.api.Direction._

class DirectionSpec extends Specification with Discipline {

  "Direction" >> {

    "Ops" >> {

      "fold" >> {
        Forwards.fold(ok, ko)
        Backwards.fold(ko, ok)
      }

    }

    "Eq" >> {
      implicit val arb: Arbitrary[Direction] = Arbitrary(Gen.oneOf(Forwards, Backwards))
      implicit val cogen: Cogen[Direction]   = Cogen[String].contramap[Direction](_.toString)
      checkAll("Direction", EqTests[Direction].eqv)
    }

  }

}
