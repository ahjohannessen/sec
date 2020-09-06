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
import org.scalacheck.Gen
import org.specs2.matcher.StandardMatchResults
import weaver.specs2compat.IOMatchers
import weaver.IOSuite
import cats.Endo
import cats.data.{NonEmptyList => Nel}
import cats.effect._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import helpers.text.snakeCaseTransformation
import sec.core._
import sec.client._
import Arbitraries._

abstract class IIOSuite extends IOSuite with IOMatchers with StandardMatchResults {
  import IIOSuite._

  type Res = EsClient[IO]

  final private val testName: String = snakeCaseTransformation(getClass.getSimpleName)

  protected def builderCfg: Endo[SingleNodeBuilder[IO]] = identity

  final def sharedResource: Resource[IO, EsClient[IO]] = for {
    log    <- Resource.liftF(Slf4jLogger.fromName[IO](snakeCaseTransformation(getClass.getSimpleName)))
    builder = builderCfg(EsClient.single[IO](endpoint))
    client <- builder
                .withChannelShutdownAwait(0.seconds)
                .withAuthority(authority)
                .withCertificate(ca.toPath)
                .withLogger(log)
                .resource
  } yield client

  def genIdentifier: String                               = sampleOfGen(Gen.identifier.suchThat(id => id.length >= 5 && id.length <= 20))
  def genStreamId: StreamId.Id                            = genStreamId(s"${testName}_")
  def genStreamId(streamPrefix: String): StreamId.Id      = sampleOfGen(idGen.genStreamIdNormal(s"$streamPrefix"))
  def genEvents(n: Int): Nel[EventData]                   = genEvents(n, eventTypeGen.defaultPrefix)
  def genEvents(n: Int, etPrefix: String): Nel[EventData] = sampleOfGen(eventdataGen.eventDataNelN(n, etPrefix))

}

object IIOSuite {

  final private val certsFolder = new File(sys.env.getOrElse("SEC_TEST_CERTS_PATH", BuildInfo.certsPath))
  final private val address     = sys.env.getOrElse("SEC_IT_TEST_HOST_ADDRESS", "127.0.0.1")
  final private val port        = sys.env.get("SEC_IT_TEST_HOST_PORT").flatMap(_.toIntOption).getOrElse(2113)
  final private val endpoint    = Endpoint(address, port)
  final private val ca          = new File(certsFolder, "ca/ca.crt")
  final private val authority   = sys.env.getOrElse("SEC_IT_TEST_AUTHORITY", "es.sec.local")

}
