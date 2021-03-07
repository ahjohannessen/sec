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
import java.util.UUID

import scala.concurrent.duration._

import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.syntax.all._
import org.typelevel.log4cats.Logger
import org.scalacheck.Gen
import sec.arbitraries._

trait SnSpec extends ClientSpec {

  def genUuid[F[_]: Sync]: F[UUID]                        = Sync[F].delay(sampleOf[UUID])
  def genIdentifier: String                               = sampleOfGen(Gen.identifier.suchThat(id => id.length >= 5 && id.length <= 20))
  def genStreamUuid[F[_]: Sync]: F[StreamId.Id]           = genUuid[F].map(id => genStreamId(id.toString.replace("-", "")))
  def genStreamId(streamPrefix: String): StreamId.Id      = sampleOfGen(idGen.genStreamIdNormal(s"$streamPrefix"))
  def genEvent: EventData                                 = sampleOfGen(eventdataGen.eventDataOne)
  def genEvents(n: Int): Nel[EventData]                   = genEvents(n, eventTypeGen.defaultPrefix)
  def genEvents(n: Int, etPrefix: String): Nel[EventData] = sampleOfGen(eventdataGen.eventDataNelN(n, etPrefix))

  final val makeResource: Resource[IO, EsClient[IO]] = SnSpec.mkClient[IO](log)

}

object SnSpec {

  final private val certsFolder = new File(sys.env.getOrElse("SEC_SIT_CERTS_PATH", BuildInfo.certsPath))
  final private val certPath    = new File(certsFolder, "ca/ca.crt").toPath
  final private val authority   = sys.env.getOrElse("SEC_SIT_AUTHORITY", "es.sec.local")
  final private val address     = sys.env.getOrElse("SEC_SIT_HOST_ADDRESS", "127.0.0.1")
  final private val port        = sys.env.get("SEC_SIT_HOST_PORT").flatMap(_.toIntOption).getOrElse(2113)

  ///

  def mkClient[F[_]: Async](log: Logger[F]): Resource[F, EsClient[F]] = EsClient
    .singleNode[F](Endpoint(address, port))
    .withAuthority(authority)
    .withCertificate(certPath)
    .withChannelShutdownAwait(0.seconds)
    .withLogger(log)
    .withOperationsRetryDisabled
    .resource

}
