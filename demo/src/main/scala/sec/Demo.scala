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

import java.io.File
import scala.concurrent.duration._
import cats.data.NonEmptySet
import cats.syntax.all._
import cats.effect._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import sec.core.Position
import sec.api._
import sec.demo.BuildInfo.certsPath
import sec.client._

object Demo extends IOApp {

  def run(args: List[String]): IO[ExitCode] = run

  val certsFolder       = new File(sys.env.getOrElse("SEC_DEMO_CERTS_PATH", certsPath))
  val ca                = new File(certsFolder, "ca/ca.crt")
  val authority: String = sys.env.getOrElse("SEC_DEMO_AUTHORITY", "es.sec.local")
  val seed: NonEmptySet[Endpoint] = NonEmptySet.of(
    getEndpoint("SEC_DEMO_ES1_ADDRESS", "SEC_DEMO_ES1_PORT", "127.0.0.1", 2114),
    getEndpoint("SEC_DEMO_ES2_ADDRESS", "SEC_DEMO_ES2_PORT", "127.0.0.1", 2115),
    getEndpoint("SEC_DEMO_ES3_ADDRESS", "SEC_DEMO_ES3_PORT", "127.0.0.1", 2116)
  )

  def getEndpoint(envAddr: String, envPort: String, fallbackAddr: String, fallbackPort: Int): Endpoint = {
    val address = sys.env.getOrElse(envAddr, fallbackAddr)
    val port    = sys.env.get(envPort).flatMap(_.toIntOption).getOrElse(fallbackPort)
    Endpoint(address, port)
  }

  ///

  def run: IO[ExitCode] = {

    val resources = for {
      l <- Resource.liftF(Slf4jLogger.fromName[IO]("Demo"))
      _ <- Resource.liftF(l.info("Starting up"))
      c <- EsClient.cluster[IO](seed, authority).withCertificate(ca.toPath).withLogger(l).resource
    } yield (l, c)

    resources.use {
      case (l, c) =>
        val read = c.streams
          .readAllForwards(Position.Start, 30)
          .evalMap(x => l.info(s"Streams.readAll ${x.eventData.eventType.show}"))
          .metered(300.millis)
          .repeat
          .take(5)

        val gossip = fs2.Stream
          .eval(c.gossip.read(None))
          .evalMap(x => l.info(s"Gossip.read: ${x.show}"))
          .metered(150.millis)
          .repeat
          .take(10)

        read.concurrently(gossip).compile.drain.as(ExitCode.Success)
    }

  }

}
