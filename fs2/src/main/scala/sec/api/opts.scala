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

import scala.concurrent.duration.*
import cats.effect.Temporal
import org.typelevel.log4cats.Logger
import sec.api.retries.*

//======================================================================================================================

final private[sec] case class Opts[F[_]](
  retryEnabled: Boolean,
  retryConfig: RetryConfig,
  retryOn: Throwable => Boolean,
  log: Logger[F]
)

private[sec] object Opts:

  extension [F[_]](opts: Opts[F])

    def run[A](fa: F[A], opName: String)(using F: Temporal[F]): F[A] =
      if opts.retryEnabled then retry[F, A](fa, opName, opts.retryConfig, opts.log)(opts.retryOn) else fa

    def logWarn(opName: String)(attempt: Int, delay: FiniteDuration, error: Throwable): F[Unit] =
      retries.logWarn(opts.retryConfig, opName, opts.log)(attempt, delay, error)

    def logError(opName: String)(error: Throwable): F[Unit] =
      retries.logError(opts.retryConfig, opName, opts.log)(error)

//======================================================================================================================
