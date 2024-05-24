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

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace
import cats.effect.*
import cats.effect.testkit.*
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.log4cats.testing.TestingLogger
import sec.api.retries.RetryConfig

class RetriesSuite extends SecEffectSuite with TestInstances:

  import RetriesSuite.Oops

  override def munitIOTimeout: Duration = 1.second

  def run[A](
    action: IO[A],
    retryConfig: RetryConfig,
    log: Logger[IO] = NoOpLogger.impl[IO],
    retryOn: Throwable => Boolean = _ => true
  ): IO[A] = retries.retry(action, "retry-suite", retryConfig, log)(retryOn)

  //

  test("immediate success") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 500.millis,
      backoffFactor = 1,
      maxAttempts   = 1,
      timeout       = None
    )

    var attempts = 0
    val action: IO[Int] = IO {
      attempts += 1
      attempts
    }

    TestControl.executeEmbed(run(action, config)).map(assertEquals(_, 1))

  }

  test("delay") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 500.millis,
      backoffFactor = 1,
      maxAttempts   = 1,
      timeout       = None
    )

    val action = run(IO.raiseError[Int](Oops), config)

    TestControl.execute(action) >>= { control =>
      for {
        _  <- assertNoResult(control)
        _  <- control.advanceAndTick(99.millis)
        _  <- assertNoResult(control)
        _  <- control.advance(1.millis)
        _  <- control.tickAll
        r1 <- assertOutcome(control)
      } yield assert(r1.fold(false, _ == Oops, _ => false))
    }

  }

  test("maxDelay") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 100.millis,
      backoffFactor = 1,
      maxAttempts   = 10,
      timeout       = None
    )

    val action = run(IO.raiseError[Int](Oops), config)

    TestControl.execute(action) >>= { control =>
      for {
        _  <- assertNoResult(control)
        _  <- control.advanceAndTick(1.second)
        _  <- control.tickAll
        r1 <- assertOutcome(control)
      } yield assert(r1.fold(false, _ == Oops, _ => false))

    }

  }

  test("backoffFactor") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 10.second,
      backoffFactor = 3,
      maxAttempts   = 3,
      timeout       = None
    )

    val action = run(IO.raiseError[Int](Oops), config)

    TestControl.execute(action) >>= { control =>
      for {
        _  <- assertNoResult(control)
        _  <- control.advanceAndTick(100.millis)
        _  <- assertNoResult(control)
        _  <- control.advanceAndTick(300.millis)
        _  <- assertNoResult(control)
        _  <- control.advanceAndTick(900.millis)
        _  <- control.tickAll
        r1 <- assertOutcome(control)
      } yield assert(r1.fold(false, _ == Oops, _ => false))

    }

  }

  test("maxAttempts") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 500.millis,
      backoffFactor = 1,
      maxAttempts   = 5,
      timeout       = None
    )

    val action = run(IO.raiseError[Int](Oops), config)

    TestControl.execute(action) >>= { control =>
      for {
        _  <- control.advance(500.millis)
        _  <- control.tickAll
        r1 <- assertOutcome(control)
      } yield assert(r1.fold(false, _ == Oops, _ => false))
    }
  }

  test("timeout") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 1.second,
      backoffFactor = 1,
      maxAttempts   = 1,
      timeout       = Some(100.millis)
    )

    val action1 = run(IO.sleep(101.millis) *> IO[Int](1), config)

    val t1 = TestControl.execute(action1) >>= { control =>
      for {
        _  <- assertNoResult(control)
        _  <- control.advance(300.millis)
        _  <- control.tickAll
        r1 <- assertOutcome(control)
      } yield assert(r1.fold(false, _ == retries.Timeout(100.millis), _ => false))
    }

    val action2 = run(IO.sleep(99.millis) *> IO[Int](1), config)

    val t2 = TestControl.execute(action2) >>= { control =>
      for {
        _  <- assertNoResult(control)
        _  <- control.advance(99.millis)
        _  <- control.tickAll
        r1 <- assertOutcome(control)
      } yield assert(r1.fold(false, _ => false, _ == 1))
    }

    t1 *> t2
  }

  test("retryOn") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 100.millis,
      backoffFactor = 2,
      maxAttempts   = 10,
      timeout       = None
    )

    val action = run(IO.raiseError[Int](Oops), config, retryOn = _ => false)

    TestControl.execute(action) >>= { control =>
      for {
        _  <- control.tick
        r1 <- assertOutcome(control)
      } yield assert(r1.fold(false, _ == Oops, _ => false))
    }

  }

  test("logs") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 100.millis,
      backoffFactor = 1,
      maxAttempts   = 10,
      timeout       = None
    )

    val logger = TestingLogger.impl[IO](warnEnabled = true, errorEnabled = true)
    val action = run(IO.raiseError[Int](Oops), config, log = logger)

    val t1 = TestControl.execute(action) >>= { control =>
      for {
        _  <- assertNoResult(control)
        _  <- control.advance(1.second)
        _  <- control.tickAll
        r1 <- assertOutcome(control)
      } yield assert(r1.fold(false, _ == Oops, _ => false))
    }

    val t2 = logger.logged.map { logs =>

      val warnings = logs.collect { case w: TestingLogger.WARN => w }
      val errors   = logs.collect { case e: TestingLogger.ERROR => e }

      val expectedWarnings = (1 until 10).map(i =>
        TestingLogger.WARN(s"retry-suite failed, attempt $i of 10, retrying in 100.0ms - Oops", None))

      val expectedErrors = List(TestingLogger.ERROR(s"retry-suite failed after 10 attempts - Oops", None))

      assertEquals(warnings.size, 9)
      assertEquals(errors.size, 1)

      assertEquals(warnings.toList, expectedWarnings.toList)
      assertEquals(errors.toList, expectedErrors)

    }

    t1 *> t2
  }

  def assertNoResult[A](control: TestControl[A]): IO[Unit] =
    assertIOBoolean(control.results.map(_.isEmpty))

  def assertOutcome[A](control: TestControl[A]): IO[Outcome[cats.Id, Throwable, A]] =
    control.results.map(_.fold(fail("Expected some result"))(identity))

object RetriesSuite:
  case object Oops extends RuntimeException("Oops") with NoStackTrace
