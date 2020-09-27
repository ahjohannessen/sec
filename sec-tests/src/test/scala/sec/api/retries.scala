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

import scala.util.control.NoStackTrace
import scala.concurrent.duration._
import cats.syntax.all._
import cats.effect._
import cats.effect.laws.util.TestContext
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.noop.NoOpLogger
import io.chrisdavenport.log4cats.testing.TestingLogger
import cats.effect.testing.specs2.CatsEffect
import org.specs2.mutable.Specification

class RetriesSpec extends Specification with CatsEffect {

  import RetriesSpec.Oops
  import sec.api.retries.{retry => _, _}

  override val Timeout = 1.second

  "retries" >> {

    def test[A](
      action: IO[A],
      retryConfig: RetryConfig,
      log: Logger[IO] = NoOpLogger.impl[IO],
      retryOn: Throwable => Boolean = _ => true
    )(implicit TC: TestContext) = {
      implicit val cs: ContextShift[IO] = IO.contextShift(TC)
      implicit val timer: Timer[IO]     = TC.timer
      retries.retry[IO, A](action, "retry-spec", retryConfig, log)(retryOn).unsafeToFuture()
    }

    ///

    "immediate success" >> {
      implicit val ec: TestContext = TestContext()

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

      test(action, config).value.map(_.toEither) should beSome(Right(1))
    }

    "delay" >> {

      implicit val ec: TestContext = TestContext()

      val config = RetryConfig(
        delay         = 100.millis,
        maxDelay      = 500.millis,
        backoffFactor = 1,
        maxAttempts   = 1,
        timeout       = None
      )

      val action = test(IO.raiseError[Int](Oops), config)

      action.value should beNone
      ec.tick(99.millis)

      action.value should beNone
      ec.tick(1.millis)

      action.value.map(_.toEither) should beSome(Oops.asLeft)
    }

    "maxDelay" >> {

      implicit val ec: TestContext = TestContext()

      val config = RetryConfig(
        delay         = 100.millis,
        maxDelay      = 100.millis,
        backoffFactor = 1,
        maxAttempts   = 10,
        timeout       = None
      )

      val action = test(IO.raiseError[Int](Oops), config)

      action.value should beNone
      ec.tick(1.second)

      action.value.map(_.toEither) should beSome(Oops.asLeft)
    }

    "backoffFactor" >> {

      implicit val ec: TestContext = TestContext()

      val config = RetryConfig(
        delay         = 100.millis,
        maxDelay      = 10.second,
        backoffFactor = 3,
        maxAttempts   = 3,
        timeout       = None
      )

      val action = test(IO.raiseError[Int](Oops), config)

      action.value should beNone
      ec.tick(100.millis)

      action.value should beNone
      ec.tick(300.millis)

      action.value should beNone
      ec.tick(900.millis)

      action.value.map(_.toEither) should beSome(Oops.asLeft)
    }

    "maxAttempts" >> {

      implicit val ec: TestContext = TestContext()

      val config = RetryConfig(
        delay         = 100.millis,
        maxDelay      = 500.millis,
        backoffFactor = 1,
        maxAttempts   = 5,
        timeout       = None
      )

      val action = test(IO.raiseError[Int](Oops), config)

      ec.tick(500.millis)

      action.value.map(_.toEither) shouldEqual Oops.asLeft.some
    }

    "timeout" >> {

      implicit val ec: TestContext = TestContext()

      val config = RetryConfig(
        delay         = 100.millis,
        maxDelay      = 1.second,
        backoffFactor = 1,
        maxAttempts   = 1,
        timeout       = Some(100.millis)
      )

      val action1 = test(IO.sleep(101.millis)(ec.timer) *> IO[Int](1), config)

      action1.value should beNone
      ec.tick(300.millis)

      action1.value.map(_.toEither) shouldEqual retries.Timeout(100.millis).asLeft.some

      val action2 = test(IO.sleep(99.millis)(ec.timer) *> IO[Int](1), config)

      action2.value should beNone
      ec.tick(99.millis)
      action2.value.map(_.toEither) should beSome(1.asRight)

    }

    "retryOn" >> {

      implicit val ec: TestContext = TestContext()

      val config = RetryConfig(
        delay         = 100.millis,
        maxDelay      = 100.millis,
        backoffFactor = 2,
        maxAttempts   = 10,
        timeout       = None
      )

      val action = test(IO.raiseError[Int](Oops), config, retryOn = _ => false)

      action.value.map(_.toEither) should beSome(Oops.asLeft)
    }

    "logs" >> {

      implicit val ec: TestContext = TestContext()

      val config = RetryConfig(
        delay         = 100.millis,
        maxDelay      = 100.millis,
        backoffFactor = 1,
        maxAttempts   = 10,
        timeout       = None
      )

      val logger = TestingLogger.impl[IO](warnEnabled = true, errorEnabled = true)
      val action = test(IO.raiseError[Int](Oops), config, log = logger)

      action.value should beNone
      ec.tick(1.second)

      action.value.map(_.toEither) should beSome(Oops.asLeft)

      logger.logged.map { logs =>

        val warnings = logs.collect { case w: TestingLogger.WARN => w }
        val errors   = logs.collect { case e: TestingLogger.ERROR => e }

        val expectedWarnings = (1 until 10).map(i =>
          TestingLogger.WARN(s"retry-spec failed, attempt $i of 10, retrying in 100.0ms - Oops", None))

        val expectedErrors = List(TestingLogger.ERROR(s"retry-spec failed after 10 attempts - Oops", None))

        warnings.size shouldEqual 9
        errors.size shouldEqual 1

        warnings.toList shouldEqual expectedWarnings.toList
        errors.toList shouldEqual expectedErrors

      }
    }

  }

}

object RetriesSpec {
  case object Oops extends RuntimeException("Oops") with NoStackTrace
}
