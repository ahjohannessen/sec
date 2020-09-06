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
package cluster

import scala.concurrent.duration._
import cats.syntax.all._
import retries._

final private[sec] case class ClusterSettings(
  maxDiscoverAttempts: Option[Int],
  retryDelay: FiniteDuration,
  retryMaxDelay: FiniteDuration,
  retryBackoffFactor: Double,
  readTimeout: FiniteDuration,
  notificationInterval: FiniteDuration,
  preference: NodePreference,
  channelShutdownAwait: FiniteDuration
)

private[sec] object ClusterSettings {

  val default: ClusterSettings = ClusterSettings(
    maxDiscoverAttempts  = None,
    retryDelay           = 100.millis,
    retryMaxDelay        = 2.seconds,
    retryBackoffFactor   = 1.25,
    readTimeout          = 5.seconds,
    notificationInterval = 100.millis,
    preference           = NodePreference.Leader,
    channelShutdownAwait = 10.seconds
  )

  implicit final class ClusterSettingsOps(val cs: ClusterSettings) extends AnyVal {

    def withMaxDiscoverAttempts(max: Option[Int]): ClusterSettings          = cs.copy(maxDiscoverAttempts = max)
    def withRetryDelay(delay: FiniteDuration): ClusterSettings              = cs.copy(retryDelay = delay)
    def withRetryMaxDelay(maxDelay: FiniteDuration): ClusterSettings        = cs.copy(retryMaxDelay = maxDelay)
    def withRetryBackoffFactor(factor: Double): ClusterSettings             = cs.copy(retryBackoffFactor = factor)
    def withReadTimeout(timeout: FiniteDuration): ClusterSettings           = cs.copy(readTimeout = timeout)
    def withNotificationInterval(interval: FiniteDuration): ClusterSettings = cs.copy(notificationInterval = interval)
    def withNodePreference(np: NodePreference): ClusterSettings             = cs.copy(preference = np)
    def withChannelShutdownAwait(await: FiniteDuration): ClusterSettings    = cs.copy(channelShutdownAwait = await)

    def retryConfig: RetryConfig = RetryConfig(
      cs.retryDelay,
      cs.retryMaxDelay,
      cs.retryBackoffFactor,
      cs.maxDiscoverAttempts.getOrElse(Int.MaxValue),
      cs.readTimeout.some
    )
  }

}
