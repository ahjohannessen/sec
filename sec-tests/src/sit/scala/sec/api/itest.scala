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
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll
import org.scalacheck.Gen
import cats.data.{NonEmptyList => Nel}
import cats.effect.testing.specs2._
import cats.effect._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import helpers.text.snakeCaseTransformation
import sec.core._
import sec.client._
import Arbitraries._

trait ITest extends Specification with CatsIO with AfterAll {

  import cats.effect.IO.ioEffect // Dotty

  import ITest._

  final private val testName                              = snakeCaseTransformation(getClass.getSimpleName)
  def genIdentifier: String                               = sampleOfGen(Gen.identifier.suchThat(id => id.length >= 5 && id.length <= 20))
  def genStreamId: StreamId.Id                            = genStreamId(s"${testName}_")
  def genStreamId(streamPrefix: String): StreamId.Id      = sampleOfGen(idGen.genStreamIdNormal(s"$streamPrefix"))
  def genEvents(n: Int): Nel[EventData]                   = genEvents(n, eventTypeGen.defaultPrefix)
  def genEvents(n: Int, etPrefix: String): Nel[EventData] = sampleOfGen(eventdataGen.eventDataNelN(n, etPrefix))

  final private lazy val (client, shutdown): (EsClient[IO], IO[Unit]) = {

    val builder = IO.delay(
      NettyChannelBuilder
        .forAddress(address, port)
        .useTransportSecurity()
        .sslContext(GrpcSslContexts.forClient().trustManager(ca).build())
        .overrideAuthority(authority)
    )

    val options = Options.default.withOperationsRetryMaxAttempts(3)

    val result: Resource[IO, EsClient[IO]] = for {
      b <- Resource.liftF(builder)
      l <- Resource.liftF(Slf4jLogger.fromName[IO]("integration-test"))
      c <- b.resourceWithShutdown[IO](mc => IO.delay(mc.shutdownNow()).void)
             .map(EsClient[IO](_, options, requiresLeader = true, l))
    } yield c

    result.allocated[IO, EsClient[IO]].unsafeRunSync()
  }

  final def esClient: EsClient[IO] = client
  final def streams: Streams[IO]   = client.streams
  final def gossip: Gossip[IO]     = client.gossip

  //

  final def afterAll(): Unit = shutdown.unsafeRunSync()

}

object ITest {

  final private val certsFolder = new File(sys.env.getOrElse("SEC_TEST_CERTS_PATH", BuildInfo.certsPath))
  final private val address     = sys.env.getOrElse("SEC_IT_TEST_HOST_ADDRESS", "127.0.0.1")
  final private val port        = sys.env.get("SEC_IT_TEST_HOST_PORT").flatMap(_.toIntOption).getOrElse(2113)
  final private val ca          = new File(certsFolder, "ca/ca.crt")
  final private val authority   = sys.env.getOrElse("SEC_IT_TEST_AUTHORITY", "es.sec.local")

}
