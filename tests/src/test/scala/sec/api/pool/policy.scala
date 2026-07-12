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
package api
package pool

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

class PoolPolicySuite extends SecScalaCheckSuite:

  test("PoolConfig rejects non-positive streams-per-channel and limits") {
    assert(PoolConfig(0).isLeft)
    assert(PoolConfig(-1).isLeft)
    assert(PoolConfig(1, Limit.Bounded(0)).isLeft)
    assert(PoolConfig(1, Limit.Unbounded(0)).isLeft)
    assert(PoolConfig(1, Limit.Bounded(1)).isRight)
    assert(PoolConfig(1, Limit.Unbounded(1)).isRight)
  }

  val slotsGen: Gen[Vector[SlotView]] =
    Gen
      .listOf(
        for
          free <- Gen.chooseNum(0, 10)
          h    <- Gen.oneOf(true, false)
        yield SlotView(0, free, h)
      )
      .map(_.zipWithIndex.map((v, i) => v.copy(index = i)).toVector)

  property("placementOrder is a permutation of all indices") {
    forAll(slotsGen) { slots =>
      PoolPolicy.placementOrder(slots).sorted == slots.indices.toList
    }
  }

  property("every healthy slot precedes every unhealthy slot") {
    forAll(slotsGen) { slots =>
      val healths = PoolPolicy.placementOrder(slots).map(i => slots(i).healthy)
      !healths.zip(healths.drop(1)).contains((false, true))
    }
  }

  property("within equal health, most-free comes first") {
    forAll(slotsGen) { slots =>
      val order = PoolPolicy.placementOrder(slots).map(slots(_))
      order.zip(order.drop(1)).forall { (a, b) =>
        a.healthy != b.healthy || a.free >= b.free
      }
    }
  }

  property("Bounded rejects exactly at max, grows below") {
    forAll(slotsGen, Gen.chooseNum(0, 20)) { (slots, max) =>
      PoolPolicy.onSaturated(slots, 5, Limit.Bounded(max)) match
        case Saturated.Reject(n) => slots.size >= max && n == slots.size
        case Saturated.Grow(_)   => slots.size < max
    }
  }

  property("Unbounded never rejects; escalates strictly past cap with occupancy snapshot") {
    forAll(slotsGen, Gen.chooseNum(0, 20), Gen.chooseNum(1, 10)) { (slots, cap, spc) =>
      PoolPolicy.onSaturated(slots, spc, Limit.Unbounded(cap)) match
        case Saturated.Reject(_)                  => false
        case Saturated.Grow(GrowEvent.Grew(n, c)) =>
          n == slots.size + 1 && n <= cap && c == n * spc
        case Saturated.Grow(GrowEvent.GrewPastCap(n, c, occ)) =>
          n == slots.size + 1 && n > cap && c == cap &&
          occ == slots.map(v => spc - v.free)
    }
  }
