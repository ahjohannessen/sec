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

private[sec] case class Options(
  connectionName: String,
  credentials: Option[UserCredentials],
  operationOptions: OperationOptions,
  connectionMode: ConnectionMode
)

private[sec] object Options {

  import ConnectionMode._

  val default: Options = Options(
    connectionName   = "sec-client",
    credentials      = UserCredentials.unsafe("admin", "changeit").some,
    operationOptions = OperationOptions.default,
    connectionMode   = Insecure
  )

  implicit final class OptionsOps(val o: Options) extends AnyVal {

    private def modifyOO(fn: OperationOptions => OperationOptions): Options =
      o.copy(operationOptions = fn(o.operationOptions))

    def withSecureMode(certChain: Path): Options                    = o.copy(connectionMode = Secure(certChain))
    def withInsecureMode: Options                                   = o.copy(connectionMode = Insecure)
    def withConnectionName(name: String): Options                   = o.copy(connectionName = name)
    def withCredentials(creds: Option[UserCredentials]): Options    = o.copy(credentials = creds)
    def withOperationsRetryDelay(delay: FiniteDuration): Options    = modifyOO(_.copy(retryDelay = delay))
    def withOperationsRetryMaxDelay(delay: FiniteDuration): Options = modifyOO(_.copy(retryMaxDelay = delay))
    def withOperationsRetryMaxAttempts(max: Int): Options           = modifyOO(_.copy(retryMaxAttempts = max))
    def withOperationsRetryBackoffFactor(factor: Double): Options   = modifyOO(_.copy(retryBackoffFactor = factor))
    def withOperationsRetryEnabled: Options                         = modifyOO(_.copy(retryEnabled = true))
    def withOperationsRetryDisabled: Options                        = modifyOO(_.copy(retryEnabled = false))
  }

}

//======================================================================================================================

sealed private[sec] trait ConnectionMode
private[sec] object ConnectionMode {
  case object Insecure                     extends ConnectionMode
  final case class Secure(certChain: Path) extends ConnectionMode
}

//======================================================================================================================

final private[sec] case class ClusterOptions(
  maxDiscoverAttempts: Option[Int],
  retryDelay: FiniteDuration,
  retryMaxDelay: FiniteDuration,
  retryBackoffFactor: Double,
  readTimeout: FiniteDuration,
  notificationInterval: FiniteDuration,
  preference: NodePreference,
  channelShutdownAwait: FiniteDuration
)

private[sec] object ClusterOptions {

  val default: ClusterOptions = ClusterOptions(
    maxDiscoverAttempts  = None,
    retryDelay           = 100.millis,
    retryMaxDelay        = 2.seconds,
    retryBackoffFactor   = 1.25,
    readTimeout          = 5.seconds,
    notificationInterval = 100.millis,
    preference           = NodePreference.Leader,
    channelShutdownAwait = 10.seconds
  )

  implicit final class ClusterOptionsOps(val co: ClusterOptions) extends AnyVal {
    def withMaxDiscoverAttempts(max: Option[Int]): ClusterOptions          = co.copy(maxDiscoverAttempts = max)
    def withRetryDelay(delay: FiniteDuration): ClusterOptions              = co.copy(retryDelay = delay)
    def withRetryMaxDelay(maxDelay: FiniteDuration): ClusterOptions        = co.copy(retryMaxDelay = maxDelay)
    def withRetryBackoffFactor(factor: Double): ClusterOptions             = co.copy(retryBackoffFactor = factor)
    def withReadTimeout(timeout: FiniteDuration): ClusterOptions           = co.copy(readTimeout = timeout)
    def withNotificationInterval(interval: FiniteDuration): ClusterOptions = co.copy(notificationInterval = interval)
    def withNodePreference(np: NodePreference): ClusterOptions             = co.copy(preference = np)
    def withChannelShutdownAwait(await: FiniteDuration): ClusterOptions    = co.copy(channelShutdownAwait = await)
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

}

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
