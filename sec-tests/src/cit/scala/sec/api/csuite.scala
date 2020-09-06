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

import java.io.File
import scala.concurrent.duration._
import org.specs2.matcher.StandardMatchResults
import weaver.specs2compat.IOMatchers
import weaver.IOSuite
import cats.data.NonEmptySet
import cats.effect._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import helpers.text.snakeCaseTransformation
import helpers.endpoint.endpointFrom
import sec.client._

trait CIOSuite extends IOSuite with IOMatchers with StandardMatchResults {
  import CIOSuite._

  type Res = EsClient[IO]

  final def sharedResource: Resource[IO, EsClient[IO]] = for {
    log <- Resource.liftF(Slf4jLogger.fromName[IO](snakeCaseTransformation(getClass.getSimpleName)))
    client <- EsClient
                .cluster[IO](seed, authority)
                .withChannelShutdownAwait(0.seconds)
                .withCertificate(ca.toPath)
                .withLogger(log)
                .resource
  } yield client
}

object CIOSuite {

  final private val certsFolder = new File(sys.env.getOrElse("SEC_CLUSTER_CERTS_PATH", BuildInfo.certsPath))
  final private val ca          = new File(certsFolder, "ca/ca.crt")
  final private val authority   = sys.env.getOrElse("SEC_CLUSTER_AUTHORITY", "es.sec.local")
  final private val seed = NonEmptySet.of(
    endpointFrom("SEC_CLUSTER_ES1_ADDRESS", "SEC_CLUSTER_ES1_PORT", "127.0.0.1", 2114),
    endpointFrom("SEC_CLUSTER_ES2_ADDRESS", "SEC_CLUSTER_ES2_PORT", "127.0.0.1", 2115),
    endpointFrom("SEC_CLUSTER_ES3_ADDRESS", "SEC_CLUSTER_ES3_PORT", "127.0.0.1", 2116)
  )

}
