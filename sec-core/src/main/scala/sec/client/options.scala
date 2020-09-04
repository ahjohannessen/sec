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
package client

import java.nio.file.Path
import scala.concurrent.duration._
import cats.syntax.all._
import api.UserCredentials
import api.OperationOptions

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
    connectionName   = "sec",
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
    def withOperationsRetryBackoffFactor(factor: Double)            = modifyOO(_.copy(retryBackoffFactor = factor))
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
