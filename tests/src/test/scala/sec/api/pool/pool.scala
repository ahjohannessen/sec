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

import scala.concurrent.duration.*
import cats.syntax.all.*
import cats.effect.{IO, Ref, Resource}
import cats.effect.std.Random
import cats.effect.testkit.TestControl
import cats.effect.unsafe.IORuntimeConfig

/** Each test maps to an invariant of [[Pool]]. Do not weaken tests to pass; fix the pool instead.
  * TestControl.executeEmbed doubles as a deadlock detector: non-terminating programs fail, not hang.
  */
class PoolSuite extends SecEffectSuite:

  /** Cancellation observed at every unmasked run-loop iteration, with preemption every other iteration.
    * This makes the cancellation invariant tests exercise far more interleavings under TestControl.
    */
  val aggressiveRt: IORuntimeConfig = IORuntimeConfig(cancelationCheckThreshold = 1, autoYieldThreshold = 2)

  def poolConfig(streamsPerChannel: Int, limit: Limit): PoolConfig =
    PoolConfig(streamsPerChannel, limit).fold(e => fail(e.getMessage), identity)

  case class Probe(created: Ref[IO, Int], closed: Ref[IO, Int], down: Ref[IO, Set[Int]])

  def probe: IO[Probe] =
    (IO.ref(0), IO.ref(0), IO.ref(Set.empty[Int])).mapN(Probe.apply)

  /** Slots are Ints; construction/closure counted; health via Ref; optional construction failure via Ref; optional
    * construction delay to widen the growth window for cancellation injection.
    */
  def testPool(
    p: Probe,
    cfg: PoolConfig,
    failMk: Option[Ref[IO, Boolean]] = None,
    mkDelay: FiniteDuration = 0.millis,
    onGrow: GrowEvent => IO[Unit] = _ => IO.unit
  ) =
    val mk = Resource.make(
      IO.sleep(mkDelay) *> failMk.fold(IO.pure(false))(_.get).flatMap { fail =>
        if fail then IO.raiseError(new RuntimeException("mk boom"))
        else p.created.updateAndGet(_ + 1)
      }
    )(_ => p.closed.update(_ + 1))
    Pool.of[IO, Int](mk, id => p.down.get.map(!_.contains(id)), cfg, onGrow)

  test("growth is exactly ceil(k / streamsPerChannel); shutdown closes all") {
    TestControl.executeEmbed {
      probe.flatMap { p =>
        testPool(p, poolConfig(3, Limit.Bounded(10))).use { pool =>
          (1 to 7).toList.traverse(_ => pool.lease.allocated).flatMap { leases =>
            pool.stats.flatMap { s =>
              IO(assertEquals(s.sum, 7)) *> IO(assertEquals(s.size, 3))
            } *> leases.traverse_(_._2)
          }
        } *> (p.created.get, p.closed.get).flatMapN((c, cl) => IO(assertEquals(c, cl)))
      }
    }
  }

  test("Bounded: loud rejection past capacity, no silent queue") {
    TestControl.executeEmbed {
      probe.flatMap { p =>
        testPool(p, poolConfig(2, Limit.Bounded(2))).use { pool =>
          (1 to 4).toList.traverse(_ => pool.lease.allocated) *>
            pool.lease.use_.attempt.flatMap {
              case Left(exceptions.SubscriptionPoolExhausted(2)) => IO.unit
              case other => IO(fail(s"expected SubscriptionPoolExhausted(2), got $other"))
            }
        }
      }
    }
  }

  test("conservation under chaos: random hold/failure/cancellation leaks nothing") {
    TestControl.executeEmbed(
      (probe, Random.scalaUtilRandom[IO]).flatMapN { (p, rnd) =>
        testPool(p, poolConfig(4, Limit.Unbounded(3))).use { pool =>
          val op: IO[Unit] =
            (rnd.betweenInt(0, 3), rnd.betweenLong(1, 50)).flatMapN { (kind, ms) =>
              kind match
                case 0 => pool.lease.use(_ => IO.sleep(ms.millis))
                case 1 => pool.lease.use(_ => IO.raiseError(new Exception("boom"))).attempt.void
                case _ =>
                  pool.lease
                    .use(_ => IO.sleep(ms.millis))
                    .start
                    .flatMap(f => IO.sleep(1.milli) *> f.cancel)
            }
          op.replicateA_(50).parReplicateA_(8) *>
            IO.sleep(1.second) *>
            pool.stats.flatMap(s => IO(assert(s.forall(_ == 0), s"leaked permits: $s")))
        }
      },
      aggressiveRt
    )
  }

  test("cancellation racing acquire itself leaks nothing") {
    TestControl.executeEmbed(
      probe.flatMap { p =>
        // With frequent preemption the cancel is often signaled while the fiber is mid-acquire.
        testPool(p, poolConfig(2, Limit.Unbounded(10))).use { pool =>
          pool.lease.use_.start.flatMap(_.cancel).replicateA_(100) *>
            pool.stats.flatMap(s => IO(assert(s.forall(_ == 0), s"leaked: $s")))
        } *> (p.created.get, p.closed.get).flatMapN((c, cl) => IO(assertEquals(c, cl)))
      },
      aggressiveRt
    )
  }

  test("cancellation during growth leaks neither permits nor channels") {
    TestControl.executeEmbed(
      probe.flatMap { p =>
        // mkDelay widens the growth window so cancellation races slot construction
        // (test injection only - real mk must not do I/O).
        testPool(p, poolConfig(2, Limit.Bounded(5)), mkDelay = 5.millis).use { pool =>
          pool.lease.use_.start
            .flatMap(f => IO.sleep(1.milli) *> f.cancel)
            .replicateA_(10) *>
            pool.stats.flatMap(s => IO(assert(s.forall(_ == 0), s"leaked permits: $s")))
        } *> (p.created.get, p.closed.get).flatMapN((c, cl) => IO(assertEquals(c, cl)))
      },
      aggressiveRt
    )
  }

  test("reconnect storm never grows the pool") {
    TestControl.executeEmbed {
      probe.flatMap { p =>
        testPool(p, poolConfig(2, Limit.Bounded(5))).use { pool =>
          pool.lease.use(_ => IO.sleep(10.millis)).replicateA_(20).parReplicateA_(4) *>
            pool.stats.flatMap(s => IO(assertEquals(s.size, 2)))
        }
      }
    }
  }

  test("mk failure propagates, state intact, pool usable after") {
    TestControl.executeEmbed {
      (probe, IO.ref(true)).flatMapN { (p, failing) =>
        testPool(p, poolConfig(2, Limit.Bounded(5)), Some(failing)).use { pool =>
          pool.lease.use_.attempt.flatMap(r => IO(assert(r.isLeft))) *>
            failing.set(false) *>
            pool.lease.use(_ => pool.stats.flatMap(s => IO(assertEquals(s.sum, 1))))
        }
      }
    }
  }

  test("onGrow failure releases the acquired permit") {
    TestControl.executeEmbed {
      (probe, IO.ref(true)).flatMapN { (p, failGrow) =>
        val hook: GrowEvent => IO[Unit] = _ =>
          failGrow.get.flatMap { fail =>
            if fail then IO.raiseError(new RuntimeException("onGrow boom"))
            else IO.unit
          }

        testPool(p, poolConfig(1, Limit.Bounded(1)), onGrow = hook).use { pool =>
          for
            failed       <- pool.lease.use_.attempt
            _            <- IO(assert(failed.isLeft))
            afterFailure <- pool.stats
            _            <- IO(assertEquals(afterFailure, Vector(0)))
            _            <- failGrow.set(false)
            _            <- pool.lease.use_
            afterSuccess <- pool.stats
            _            <- IO(assertEquals(afterSuccess, Vector(0)))
          yield ()
        } *> (p.created.get, p.closed.get).flatMapN((created, closed) => IO(assertEquals(created, closed)))
      }
    }
  }

  test("unhealthy slot deprioritized despite equal free permits") {
    TestControl.executeEmbed {
      probe.flatMap { p =>
        testPool(p, poolConfig(2, Limit.Bounded(5))).use { pool =>
          for
            l1 <- pool.lease.allocated    //          slot 1
            _  <- pool.lease.allocated    //          slot 1 full
            _  <- pool.lease.allocated    //          grows slot 2 (one free)
            _  <- l1._2                   //                         slot 1: one free again
            _  <- p.down.update(_ + 1)    //          slot 1 unhealthy
            id <- pool.lease.use(IO.pure) //       equal free -> must pick healthy slot 2
          yield assertEquals(id, 2)
        }
      }
    }
  }

  test("close is a state transition: a late lease fails loudly instead of growing into the void") {
    TestControl.executeEmbed(
      probe.flatMap { p =>
        testPool(p, poolConfig(2, Limit.Bounded(5))).allocated.flatMap { case (pool, close) =>
          pool.lease.use_ *> close *>
            pool.lease.use_.attempt.flatMap {
              case Left(_: IllegalStateException) => IO.unit
              case other                          => IO(fail(s"expected IllegalStateException, got $other"))
            } *> (p.created.get, p.closed.get).flatMapN((c, cl) => IO(assertEquals(c, cl)))
        }
      },
      aggressiveRt
    )
  }

  test("close racing an in-flight acquire: the slot is either committed and closed, or never built") {
    TestControl.executeEmbed(
      probe.flatMap { p =>
        // The acquire is mid-construction (mkDelay) when close begins. Close must serialize with
        // it through the cell's lock: the committed slot lands in close's snapshot and is closed.
        // A snapshot read taken before the commit would miss the slot and leak the channel.
        testPool(p, poolConfig(2, Limit.Bounded(5)), mkDelay = 5.millis).allocated.flatMap { case (pool, close) =>
          pool.lease.use(_ => IO.sleep(10.millis)).start.flatMap { f =>
            IO.sleep(1.milli) *> close *> f.join.void
          } *> (p.created.get, p.closed.get).flatMapN((c, cl) => IO(assertEquals(c, cl)))
        }
      },
      aggressiveRt
    )
  }
