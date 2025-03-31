/*
 * Copyright 2020 Scala Event Sourcing client for KurrentDB
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
import cats.Order
import cats.effect.*
import fs2.Stream
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.log4cats.testing.TestingLogger
import sec.api.Direction.Forwards
import sec.api.Streams.*
import sec.api.retries.RetryConfig

class StreamsWithRetrySuite extends SecEffectSuite:

  import StreamsSpec.*

  test("immediate success") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 500.millis,
      backoffFactor = 1,
      maxAttempts   = 5,
      timeout       = None
    )

    IO.defer {
      var attempts = 0
      val action: IO[Int] = IO {
        attempts += 1
        attempts
      }

      val opts   = Opts[IO](retryEnabled = true, config, _ => true, NoOpLogger.impl[IO])
      val result = withRetry[IO, Int, Int](0, _ => Stream.eval(action), identity, opts, "with-retry", Forwards)

      assertIO(result.compile.toList, List(1))
    }
  }

  test("eventual success") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 500.millis,
      backoffFactor = 1,
      maxAttempts   = 5,
      timeout       = None
    )

    IO.defer {

      var failures, successes = 0
      val action: IO[Int] = IO {
        if (failures == 5) {
          successes += 1
          successes
        } else {
          failures += 1
          throw RetryErr()
        }
      }

      val opts   = Opts[IO](retryEnabled = true, config, _ => true, NoOpLogger.impl[IO])
      val result = withRetry[IO, Int, Int](0, _ => Stream.eval(action), identity, opts, "with-retry", Forwards)

      result.compile.toList.map { r =>
        assertEquals(failures, 5)
        assertEquals(successes, 1)
        assertEquals(r, List(1))
      }

    }

  }

  test("maxAttempts") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 500.millis,
      backoffFactor = 1,
      maxAttempts   = 4,
      timeout       = None
    )

    IO.defer {

      var failures = 0
      val action: IO[Int] = IO {
        failures += 1
        throw RetryErr(failures.toString)
      }

      val opts   = Opts[IO](retryEnabled = true, config, _ => true, NoOpLogger.impl[IO])
      val result = withRetry[IO, Int, Int](0, _ => Stream.eval(action), identity, opts, "with-retry", Forwards)

      result.compile.toList.attempt.map { att =>
        assertEquals(att, Left(RetryErr("5")))
        assertEquals(failures, 5)
      }
    }
  }

  test("retryOn") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 100.millis,
      backoffFactor = 1,
      maxAttempts   = 10,
      timeout       = None
    )

    IO.defer {

      var failures, successes = 0
      val action = IO {
        if (failures == 5) {
          failures += 1; throw RetryErr("fatal")
        } else if (failures > 5) {
          successes += 1; successes
        } else {
          failures += 1; throw RetryErr()
        }
      }

      val opts   = Opts[IO](retryEnabled = true, config, _.getMessage != "fatal", NoOpLogger.impl[IO])
      val result = withRetry[IO, Int, Int](0, _ => Stream.eval(action), identity, opts, "with-retry", Forwards)

      result.compile.toList.attempt.map { att =>

        assertEquals(att, Left(RetryErr("fatal")))
        assertEquals(successes, 0)
        assertEquals(failures, 6)

      }
    }

  }

  test("delay / maxDelay / backoffFactor") {

    val unit = 200
    val config = RetryConfig(
      delay         = unit.millis,
      maxDelay      = 1600.millis,
      backoffFactor = 2,
      maxAttempts   = 5,
      timeout       = None
    )

    IO.defer {

      val delays = scala.collection.mutable.ListBuffer.empty[Long]

      def getDelays: List[Long] = delays
        .synchronized(delays.toList)
        .sliding(2)
        .map(s => (s.tail.head - s.head) / unit)
        .toList

      val action: IO[Int] = {
        val start = System.currentTimeMillis()
        IO {
          delays.synchronized(delays += System.currentTimeMillis() - start)
          throw RetryErr()
        }
      }

      val opts   = Opts[IO](retryEnabled = true, config, _ => true, NoOpLogger.impl[IO])
      val result = withRetry[IO, Int, Int](0, _ => Stream.eval(action), identity, opts, "with-retry", Forwards)

      result.compile.lastOrError.attempt.map { att =>
        assertEquals(getDelays, List[Long](1, 2, 4, 8, 8))
        assertEquals(att, Left(RetryErr()))
      }
    }
  }

  test("retryEnabled") {

    val config = RetryConfig(
      delay         = 200.millis,
      maxDelay      = 1.second,
      backoffFactor = 1,
      maxAttempts   = 10,
      timeout       = None
    )

    var failures, successes = 0
    val action: IO[Int] = IO {
      if (failures == 5) {
        successes += 1
        successes
      } else {
        failures += 1
        throw RetryErr(s"successes: $successes")
      }
    }

    val opts   = Opts[IO](retryEnabled = false, config, _ => true, NoOpLogger.impl[IO])
    val result = withRetry[IO, Int, Int](0, _ => Stream.eval(action), identity, opts, "with-retry", Forwards)

    assertIO(result.compile.toList.attempt, Left(RetryErr("successes: 0")))
  }

  test("logs") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 500.millis,
      backoffFactor = 1,
      maxAttempts   = 5,
      timeout       = None
    )

    val action = IO.raiseError[Int](RetryErr("OhNoes"))
    val logger = TestingLogger.impl[IO](warnEnabled = true, errorEnabled = true)
    val opts   = Opts[IO](retryEnabled = true, config, _ => true, logger)

    IO.defer {

      val stream = withRetry[IO, Int, Int](0, _ => Stream.eval(action), identity, opts, "with-retry", Forwards)

      assertIO(stream.compile.toList.attempt, Left(RetryErr("OhNoes")))

    } *> logger.logged.map { logs =>

      val warnings = logs.collect { case w: TestingLogger.WARN => w }
      val errors   = logs.collect { case e: TestingLogger.ERROR => e }

      val expectedWarnings = (1 until 5).map(i =>
        TestingLogger.WARN(s"with-retry failed, attempt $i of 5, retrying in 100.0ms - OhNoes", None))

      val expectedErrors = List(TestingLogger.ERROR(s"with-retry failed after 5 attempts - OhNoes", None))

      assertEquals(warnings.size, 4)
      assertEquals(errors.size, 1)

      assertEquals(warnings.toList, expectedWarnings.toList)
      assertEquals(errors.toList, expectedErrors)

    }

  }

  group("only emit elements that can pass through") {

    val config = RetryConfig(
      delay         = 100.millis,
      maxDelay      = 500.millis,
      backoffFactor = 1,
      maxAttempts   = 5,
      timeout       = None
    )

    val opts = Opts[IO](retryEnabled = true, config, _ => true, NoOpLogger.impl[IO])

    def run[T: Order](
      first: T,
      before: Stream[IO, T],
      after: Stream[IO, T],
      direction: Direction,
      emitFrom: Boolean
    ): IO[List[T]] = {

      var hasFailed = false

      def source(from: T): Stream[IO, T] = {
        val init = Stream.emit(from).filter(_ => emitFrom)
        if (hasFailed)
          init ++ after
        else
          Stream.eval(IO.delay { hasFailed = true }) >> init ++ before ++ Stream.raiseError[IO](RetryErr())
      }

      withRetry[IO, T, T](first, source, identity, opts, "with-retry", direction).compile.toList
    }

    test("forwards") {

      def runForwards[T: Order](first: T, before: Stream[IO, T], after: Stream[IO, T], emitFrom: Boolean = true) =
        run[T](first, before, after, Direction.Forwards, emitFrom)

      val f1Before = Stream.empty
      val f1After  = Stream.empty

      val f1a = assertIO(runForwards(1, f1Before, f1After), List(1))
      val f1b = assertIO(runForwards(1, f1Before, f1After, false), Nil)
      val f1c = assertIO(runForwards(Option.empty[Int], f1Before, f1After), List(None))
      val f1d = assertIO(runForwards(Option(1), f1Before, f1After), List(Some(1)))
      val f1e = assertIO(runForwards(Option(1), f1Before, f1After, false), Nil)

      val f2Before = Stream(1, 1, 2)
      val f2After  = Stream(2, 3, 4, 5)
      val f2All    = List(0, 1, 2, 3, 4, 5)

      val f2BeforeOpt = f2Before.map(Option(_))
      val f2AfterOpt  = f2After.map(Option(_))
      val f2AllOpt    = f2All.map(Option(_))

      val f2a = assertIO(runForwards(0, f2Before, f2After), f2All)
      val f2b = assertIO(runForwards(0, f2Before, f2After, emitFrom = false), f2All.tail)
      val f2c = assertIO(runForwards(Option(0), f2BeforeOpt, f2AfterOpt), f2AllOpt)
      val f2d = assertIO(runForwards(Option(0), f2BeforeOpt, f2AfterOpt, emitFrom = false), f2AllOpt.tail)

      f1a *> f1b *> f1c *> f1d *> f1e *> f2a *> f2b *> f2c *> f2d

    }

    test("backwards") {

      def runBackwards[T: Order](first: T, before: Stream[IO, T], after: Stream[IO, T], emitFrom: Boolean = true) =
        run[T](first, before, after, Direction.Backwards, emitFrom)

      val b1 = assertIO(runBackwards(1, Stream.empty, Stream.empty), List(1))
      val b2 = assertIO(runBackwards(1, Stream.empty, Stream.empty, false), Nil)
      val b3 = assertIO(runBackwards(5, Stream(4, 3, 2), Stream(2, 1, 0)), List(5, 4, 3, 2, 1, 0))
      val b4 = assertIO(runBackwards(5, Stream(4, 3, 2), Stream(2, 1, 0), false), List(4, 3, 2, 1, 0))

      b1 *> b2 *> b3 *> b4

    }
  }

object StreamsSpec:
  final case class RetryErr(msg: String = "") extends RuntimeException(msg) with NoStackTrace
