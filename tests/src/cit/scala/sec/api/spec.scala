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

import java.io.File
import scala.concurrent.duration._
import cats.data.NonEmptySet
import cats.effect._
import org.typelevel.log4cats.Logger
import helpers.endpoint.endpointFrom
import org.specs2.specification.Retries

trait CSpec extends ClientSpec with Retries {
  override def sleep: Duration                       = 500.millis
  final val makeResource: Resource[IO, EsClient[IO]] = CSpec.mkClient[IO](log)
}

object CSpec {

  final private val certsFolder = new File(sys.env.getOrElse("SEC_CIT_CERTS_PATH", BuildInfo.certsPath))
  final private val ca          = new File(certsFolder, "ca/ca.crt")
  final private val authority   = sys.env.getOrElse("SEC_CIT_AUTHORITY", "es.sec.local")
  final private val seed = NonEmptySet.of(
    endpointFrom("SEC_CLUSTER_ES1_ADDRESS", "SEC_CIT_ES1_PORT", "127.0.0.1", 2114),
    endpointFrom("SEC_CLUSTER_ES2_ADDRESS", "SEC_CIT_ES2_PORT", "127.0.0.1", 2115),
    endpointFrom("SEC_CLUSTER_ES3_ADDRESS", "SEC_CIT_ES3_PORT", "127.0.0.1", 2116)
  )

  ///

  def mkClient[F[_]: Async](log: Logger[F]): Resource[F, EsClient[F]] = EsClient
    .cluster[F](seed, authority)
    .withChannelShutdownAwait(0.seconds)
    .withCertificate(ca)
    .withLogger(log)
    .withOperationsRetryDisabled
    .resource

}
