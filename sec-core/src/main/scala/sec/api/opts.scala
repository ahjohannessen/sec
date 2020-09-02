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

import scala.concurrent.duration._
import cats.effect.{Concurrent, Timer}
import io.chrisdavenport.log4cats.Logger
import sec.api.retries._

//======================================================================================================================

final private[sec] case class Opts[F[_]](
  retryEnabled: Boolean,
  retryConfig: RetryConfig,
  retryOn: Throwable => Boolean,
  log: Logger[F]
)

private[sec] object Opts {

  implicit final class OptsOps[F[_]](val opts: Opts[F]) extends AnyVal {

    def run[A](fa: F[A], opName: String)(implicit F: Concurrent[F], T: Timer[F]): F[A] =
      opts.retryEnabled.fold(
        retry[F, A](fa, opName, opts.retryConfig, opts.log)(opts.retryOn),
        fa
      )

    def logWarn(opName: String)(attempt: Int, delay: FiniteDuration, error: Throwable): F[Unit] =
      opts.retryConfig.logWarn[F](opName, opts.log)(attempt, delay, error)

    def logError(opName: String)(error: Throwable): F[Unit] =
      opts.retryConfig.logError[F](opName, opts.log)(error)

  }
}

//======================================================================================================================

final private[sec] case class OperationOptions(
  retryEnabled: Boolean,
  retryDelay: FiniteDuration,
  retryMaxDelay: FiniteDuration,
  retryBackoffFactor: Double,
  retryMaxAttempts: Int
)

private[sec] object OperationOptions {

  val default: OperationOptions = OperationOptions(
    retryEnabled       = true,
    retryDelay         = 250.millis,
    retryMaxDelay      = 5.seconds,
    retryBackoffFactor = 1.5,
    retryMaxAttempts   = 100
  )

  implicit final class OperationOptionsOps(val oo: OperationOptions) extends AnyVal {

    def retryConfig: RetryConfig = RetryConfig(
      delay         = oo.retryDelay,
      maxDelay      = oo.retryMaxDelay,
      backoffFactor = oo.retryBackoffFactor,
      maxAttempts   = oo.retryMaxAttempts,
      timeout       = None
    )

  }

}
