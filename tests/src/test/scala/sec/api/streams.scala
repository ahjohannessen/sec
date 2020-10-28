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

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import cats.Order
import cats.effect._
import cats.effect.testing.specs2.CatsIO
import fs2.Stream
import io.chrisdavenport.log4cats.noop.NoOpLogger
import io.chrisdavenport.log4cats.testing.TestingLogger
import org.specs2.mutable.Specification
import sec.api.Direction.Forwards
import sec.api.Streams._
import sec.api.retries.RetryConfig

class StreamsSpec extends Specification with CatsIO {

  import StreamsSpec._

  "Streams.withRetry" >> {

    "immediate success" >> {

      val config = RetryConfig(
        delay         = 100.millis,
        maxDelay      = 500.millis,
        backoffFactor = 1,
        maxAttempts   = 5,
        timeout       = None
      )

      IO.suspend {
        var attempts = 0
        val action: IO[Int] = IO {
          attempts += 1
          attempts
        }

        val opts   = Opts[IO](retryEnabled = true, config, _ => true, NoOpLogger.impl[IO])
        val result = withRetry[IO, Int, Int](0, _ => Stream.eval(action), identity, opts, "with-retry", Forwards)

        result.compile.toList.map(_ shouldEqual List(1))
      }
    }

    "eventual success" >> {

      val config = RetryConfig(
        delay         = 100.millis,
        maxDelay      = 500.millis,
        backoffFactor = 1,
        maxAttempts   = 5,
        timeout       = None
      )

      IO.suspend {

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
          failures shouldEqual 5
          successes shouldEqual 1
          r shouldEqual List(1)
        }

      }

    }

    "maxAttempts" >> {

      val config = RetryConfig(
        delay         = 100.millis,
        maxDelay      = 500.millis,
        backoffFactor = 1,
        maxAttempts   = 4,
        timeout       = None
      )

      IO.suspend {

        var failures = 0
        val action: IO[Int] = IO {
          failures += 1
          throw RetryErr(failures.toString)
        }

        val opts   = Opts[IO](retryEnabled = true, config, _ => true, NoOpLogger.impl[IO])
        val result = withRetry[IO, Int, Int](0, _ => Stream.eval(action), identity, opts, "with-retry", Forwards)

        result.compile.toList.attempt.map { att =>
          att should beLeft(RetryErr("5"))
          failures shouldEqual 5
        }
      }
    }

    "retryOn" >> {

      val config = RetryConfig(
        delay         = 100.millis,
        maxDelay      = 100.millis,
        backoffFactor = 1,
        maxAttempts   = 10,
        timeout       = None
      )

      IO.suspend {

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

          att should beLeft(RetryErr("fatal"))
          successes shouldEqual 0
          failures shouldEqual 6

        }
      }

    }

    "delay / maxDelay / backoffFactor" >> {

      val unit = 200
      val config = RetryConfig(
        delay         = unit.millis,
        maxDelay      = 1600.millis,
        backoffFactor = 2,
        maxAttempts   = 5,
        timeout       = None
      )

      IO.suspend {

        val delays = scala.collection.mutable.ListBuffer.empty[Long]

        def getDelays = delays
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
          getDelays shouldEqual List(1, 2, 4, 8, 8)
          att should beLeft(RetryErr())
        }
      }
    }

    "retryEnabled" >> {

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

      result.compile.toList.attempt.map(_ should beLeft(RetryErr("successes: 0")))
    }

    "logs" >> {

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

      IO.suspend {

        val stream = withRetry[IO, Int, Int](0, _ => Stream.eval(action), identity, opts, "with-retry", Forwards)

        stream.compile.toList.attempt.map(_ should beLeft(RetryErr("OhNoes")))

      } *> logger.logged.map { logs =>

        val warnings = logs.collect { case w: TestingLogger.WARN => w }
        val errors   = logs.collect { case e: TestingLogger.ERROR => e }

        val expectedWarnings = (1 until 5).map(i =>
          TestingLogger.WARN(s"with-retry failed, attempt $i of 5, retrying in 100.0ms - OhNoes", None))

        val expectedErrors = List(TestingLogger.ERROR(s"with-retry failed after 5 attempts - OhNoes", None))

        warnings.size shouldEqual 4
        errors.size shouldEqual 1

        warnings.toList shouldEqual expectedWarnings.toList
        errors.toList shouldEqual expectedErrors

      }

    }

    "only emit elements that can pass through" >> {

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

      def runForwards[T: Order](first: T, before: Stream[IO, T], after: Stream[IO, T], emitFrom: Boolean = true) =
        run[T](first, before, after, Direction.Forwards, emitFrom)

      def runBackwards[T: Order](first: T, before: Stream[IO, T], after: Stream[IO, T], emitFrom: Boolean = true) =
        run[T](first, before, after, Direction.Backwards, emitFrom)

      // Forwards

      val f1Before = Stream.empty
      val f1After  = Stream.empty

      val f1a = runForwards(1, f1Before, f1After).map(_ shouldEqual List(1))
      val f1b = runForwards(1, f1Before, f1After, false).map(_ shouldEqual Nil)
      val f1c = runForwards(Option.empty[Int], f1Before, f1After).map(_ shouldEqual List(None))
      val f1d = runForwards(Option(1), f1Before, f1After).map(_ shouldEqual List(Some(1)))
      val f1e = runForwards(Option(1), f1Before, f1After, false).map(_ shouldEqual Nil)

      val f2Before = Stream(1, 1, 2)
      val f2After  = Stream(2, 3, 4, 5)
      val f2All    = List(0, 1, 2, 3, 4, 5)

      val f2BeforeOpt = f2Before.map(Option(_))
      val f2AfterOpt  = f2After.map(Option(_))
      val f2AllOpt    = f2All.map(Option(_))

      val f2a = runForwards(0, f2Before, f2After).map(_ shouldEqual f2All)
      val f2b = runForwards(0, f2Before, f2After, emitFrom = false).map(_ shouldEqual f2All.tail)
      val f2c = runForwards(Option(0), f2BeforeOpt, f2AfterOpt).map(_ shouldEqual f2AllOpt)
      val f2d = runForwards(Option(0), f2BeforeOpt, f2AfterOpt, emitFrom = false).map(_ shouldEqual f2AllOpt.tail)

      val forwards = f1a *> f1b *> f1c *> f1d *> f1e *> f2a *> f2b *> f2c *> f2d

      // Backwards

      val b1 = runBackwards(1, Stream.empty, Stream.empty).map(_ shouldEqual List(1))
      val b2 = runBackwards(1, Stream.empty, Stream.empty, false).map(_ shouldEqual Nil)
      val b3 = runBackwards(5, Stream(4, 3, 2), Stream(2, 1, 0)).map(_ shouldEqual List(5, 4, 3, 2, 1, 0))
      val b4 = runBackwards(5, Stream(4, 3, 2), Stream(2, 1, 0), false).map(_ shouldEqual List(4, 3, 2, 1, 0))

      val backwards = b1 *> b2 *> b3 *> b4

      forwards *> backwards

    }

  }

}

object StreamsSpec {
  final case class RetryErr(msg: String = "") extends RuntimeException(msg) with NoStackTrace
}
