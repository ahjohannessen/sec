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

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.control.NonFatal
import cats.effect.implicits.*
import cats.effect.*
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import sec.utilities.*

private[sec] object retries:

//======================================================================================================================

  sealed abstract case class RetryConfig(
    delay: FiniteDuration,
    maxDelay: FiniteDuration,
    backoffFactor: Double,
    maxAttempts: Int,
    timeout: Option[FiniteDuration]
  )

  object RetryConfig:

    def apply(
      delay: FiniteDuration,
      maxDelay: FiniteDuration,
      backoffFactor: Double,
      maxAttempts: Int,
      timeout: Option[FiniteDuration]
    ): RetryConfig =
      new RetryConfig(delay.min(maxDelay), maxDelay, backoffFactor.max(1), math.max(maxAttempts, 1), timeout) {}

    extension (c: RetryConfig)
      def nextDelay(d: FiniteDuration): FiniteDuration =
        (d * c.backoffFactor).min(c.maxDelay) match
          case f: FiniteDuration    => f
          case _: Duration.Infinite => c.maxDelay

//======================================================================================================================

  final case class Timeout(after: FiniteDuration) extends RuntimeException(s"Timed out after ${format(after)}.")

//======================================================================================================================

  def retry[F[_]: Temporal, A](
    action: F[A],
    actionName: String,
    retryConfig: RetryConfig,
    log: Logger[F]
  )(retryOn: Throwable => Boolean): F[A] =
    import retryConfig.*

    def withTimeout(to: FiniteDuration): F[A] =
      action.timeout(to).adaptError { case _: TimeoutException => Timeout(to) }

    val fa          = timeout.fold(action)(withTimeout)
    val logWarn     = retries.logWarn[F](retryConfig, actionName, log)
    val logError    = retries.logError[F](retryConfig, actionName, log)
    val maxAttempts = retryConfig.maxAttempts
    val nextDelay   = retryConfig.nextDelay

    def run(attempts: Int, d: FiniteDuration): F[A] = fa.recoverWith {
      case NonFatal(t) if retryOn(t) =>
        if (attempts <= maxAttempts)
          logWarn(attempts, d, t).whenA(attempts < maxAttempts) *>
            Temporal[F].sleep(d) *> run(attempts + 1, nextDelay(d))
        else
          logError(t) *> t.raiseError[F, A]
    }

    run(1, retryConfig.delay)

//======================================================================================================================

  def logWarn[F[_]](cfg: RetryConfig, action: String, log: Logger[F])(
    attempt: Int,
    delay: FiniteDuration,
    th: Throwable
  ): F[Unit] =
    log.warn(s"$action failed, attempt $attempt of ${cfg.maxAttempts}, retrying in ${format(delay)} - ${th.getMessage}")

  def logError[F[_]](cfg: RetryConfig, action: String, log: Logger[F])(th: Throwable): F[Unit] =
    log.error(s"$action failed after ${cfg.maxAttempts} attempts - ${th.getMessage}")

//======================================================================================================================
