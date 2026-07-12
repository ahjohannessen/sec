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
import cats.effect.IO
import cats.effect.testkit.TestControl
import org.typelevel.log4cats.testing.TestingLogger
import org.typelevel.log4cats.testing.TestingLogger.{INFO, WARN}

class CallCounterSuite extends SecEffectSuite:

  def counted(spc: Int): IO[(TestingLogger[IO], CallCounter[IO])] =
    val log = TestingLogger.impl[IO]()
    CallCounter.of[IO](spc, log).tupleLeft(log)

  def warns(log: TestingLogger[IO]): IO[Int]   = log.logged.map(_.count(_.isInstanceOf[WARN]))
  def resumes(log: TestingLogger[IO]): IO[Int] = log.logged.map(_.count(_.isInstanceOf[INFO]))

  test("warns once crossing 90%, resumes once at 75%, hysteresis in between") {
    TestControl.executeEmbed {
      counted(10).flatMap { (log, c) =>
        // warnAt = 9, resumeAt = 7
        c.count(IO.sleep(10.millis)).start.replicateA(9).flatMap { fibers =>
          for
            _ <- IO.sleep(1.milli)
            w <- warns(log)
            _ <- IO(assertEquals(w, 1))                            //         9 in flight: warned exactly once
            _ <- c.count(IO.unit)                                  //               bounce 9 -> 10 -> 9: no second warn, no resume
            _ <- warns(log).flatMap(w => IO(assertEquals(w, 1)))
            _ <- resumes(log).flatMap(r => IO(assertEquals(r, 0))) // 9 > resumeAt
            _ <- fibers.traverse_(_.joinWithNever)                 //              all done: 0 in flight
            r <- resumes(log)
            _ <- IO(assertEquals(r, 1))                            //         resumed exactly once on the way down
          yield ()
        }
      }
    }
  }

  test("re-warns after a full warn-resume cycle") {
    TestControl.executeEmbed {
      counted(10).flatMap { (log, c) =>
        val spike = c.count(IO.sleep(1.milli)).start.replicateA(9).flatMap(_.traverse_(_.joinWithNever))
        spike *> spike *>
          (warns(log), resumes(log)).flatMapN { (w, r) =>
            IO(assertEquals(w, 2)) *> IO(assertEquals(r, 2))
          }
      }
    }
  }

  test("decrements on error and cancellation - counter conserves") {
    TestControl.executeEmbed {
      counted(4).flatMap { (log, c) =>
        val ok        = c.count(IO.sleep(2.millis))
        val boom      = c.count(IO.raiseError(new RuntimeException("boom"))).attempt.void
        val cancelled = c.count(IO.sleep(10.millis)).start.flatMap(f => IO.sleep(1.milli) *> f.cancel)
        val stream    = c.countStream(fs2.Stream.eval(IO.sleep(2.millis))).compile.drain

        // Sequential rounds never exceed one in-flight call, so any warn can only
        // come from missed decrements accumulating to warnAt (3) - a leak detector.
        (ok *> boom *> cancelled *> stream).replicateA_(25) *>
          c.count(IO.unit) *> warns(log).flatMap(w => IO(assertEquals(w, 0)))
      }
    }
  }

  test("streams are counted for their full lifetime") {
    TestControl.executeEmbed {
      counted(2).flatMap { (log, c) =>
        // warnAt = 1: a single in-flight stream trips the warn immediately
        c.countStream(fs2.Stream.eval(IO.sleep(5.millis))).compile.drain *>
          (warns(log), resumes(log)).flatMapN { (w, r) =>
            IO(assertEquals(w, 1)) *> IO(assertEquals(r, 1))
          }
      }
    }
  }
