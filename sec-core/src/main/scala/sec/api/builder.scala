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

import java.nio.file.Path
import scala.concurrent.duration._
import cats.syntax.all._

//======================================================================================================================

final private[sec] case class ChannelBuilderParams(
  targetOrEndpoint: Either[String, Endpoint],
  mode: ConnectionMode
)

private[sec] object ChannelBuilderParams {

  def apply(target: String, cm: ConnectionMode): ChannelBuilderParams =
    ChannelBuilderParams(target.asLeft, cm)

  def apply(endpoint: Endpoint, cm: ConnectionMode): ChannelBuilderParams =
    ChannelBuilderParams(endpoint.asRight, cm)

}

//======================================================================================================================

private[sec] trait OptionsBuilder[B <: OptionsBuilder[B]] {

  private[sec] def modOptions(fn: Options => Options): B

  def withCertificate(value: Path): B                       = modOptions(_.withSecureMode(value))
  def withConnectionName(value: String): B                  = modOptions(_.withConnectionName(value))
  def withCredentials(value: Option[UserCredentials]): B    = modOptions(_.withCredentials(value))
  def withOperationsRetryDelay(value: FiniteDuration): B    = modOptions(_.withOperationsRetryDelay(value))
  def withOperationsRetryMaxDelay(value: FiniteDuration): B = modOptions(_.withOperationsRetryMaxDelay(value))
  def withOperationsRetryMaxAttempts(value: Int): B         = modOptions(_.withOperationsRetryMaxAttempts(value))
  def withOperationsRetryBackoffFactor(value: Double): B    = modOptions(_.withOperationsRetryBackoffFactor(value))
  def withOperationsRetryEnabled: B                         = modOptions(_.withOperationsRetryEnabled)
  def withOperationsRetryDisabled: B                        = modOptions(_.withOperationsRetryDisabled)
}

//======================================================================================================================

private[sec] trait ClusterOptionsBuilder[B <: ClusterOptionsBuilder[B]] {

  private[sec] def modCOptions(fn: ClusterOptions => ClusterOptions): B

  def withClusterMaxDiscoveryAttempts(value: Int): B            = modCOptions(_.withMaxDiscoverAttempts(value.some))
  def withClusterRetryDelay(value: FiniteDuration): B           = modCOptions(_.withRetryDelay(value))
  def withClusterRetryMaxDelay(value: FiniteDuration): B        = modCOptions(_.withRetryMaxDelay(value))
  def withClusterRetryBackoffFactor(value: Double): B           = modCOptions(_.withRetryBackoffFactor(value))
  def withClusterReadTimeout(value: FiniteDuration): B          = modCOptions(_.withReadTimeout(value))
  def withClusterNotificationInterval(value: FiniteDuration): B = modCOptions(_.withNotificationInterval(value))
  def withClusterNodePreference(value: NodePreference): B       = modCOptions(_.withNodePreference(value))
  def withChannelShutdownAwait(value: FiniteDuration): B        = modCOptions(_.withChannelShutdownAwait(value))
}

//======================================================================================================================
