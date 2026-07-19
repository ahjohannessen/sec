/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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

import java.net.{HttpURLConnection, URI}
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.util.Try
import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import sec.arbitraries.*

/** A single KurrentDB node under test-controlled fault injection. Insecure and disk-backed on purpose: TLS is
  * irrelevant to the transport behavior under test, and [[KdbNode.restart]] must preserve stream state - a mem-db
  * reset would legitimately stall subscribers holding positions.
  */
final class KdbNode private (container: KdbNode.Kdb):

  def endpoint: Endpoint = Endpoint(container.getHost, container.getMappedPort(KdbNode.grpcPort))

  /** Restarts the node in-place via the docker daemon - same container, same filesystem layer, same port mapping -
    * and awaits readiness. Every live gRPC connection receives a real GOAWAY / connection loss: the fault the
    * subscription pool must absorb by re-placing onto freed permits instead of growing or stalling.
    */
  def restart: IO[Unit] =
    IO.blocking(container.getDockerClient.restartContainerCmd(container.getContainerId).exec()).void *>
      IO.defer(awaitReady)

  private def awaitReady: IO[Unit] =
    val url     = s"http://${container.getHost}:${container.getMappedPort(KdbNode.grpcPort)}/health/live"
    val timeout = 30.seconds
    val probe = IO.blocking {
      val conn = URI.create(url).toURL.openConnection().asInstanceOf[HttpURLConnection]
      conn.setConnectTimeout(1000)
      conn.setReadTimeout(1000)
      try conn.getResponseCode
      finally conn.disconnect()
    }

    IO.monotonic.flatMap { started =>
      def loop: IO[Unit] =
        probe.attempt.flatMap {
          case Right(status) if status >= 200 && status < 300 => IO.unit
          case last                                           =>
            IO.monotonic.flatMap { now =>
              if now - started >= timeout then
                readinessFailure(url, timeout, last).flatMap(error => IO.raiseError[Unit](error))
              else IO.sleep(250.millis) *> loop
            }
        }

      loop
    }

  private def readinessFailure(
    url: String,
    timeout: FiniteDuration,
    last: Either[Throwable, Int]
  ): IO[TimeoutException] =
    IO.blocking {
      val probeDetail = last.fold(
        error => s"${error.getClass.getName}: ${Option(error.getMessage).getOrElse("<no message>")}",
        status => s"HTTP $status"
      )
      val running = Try(container.isRunning).fold(
        error => s"unknown (${Option(error.getMessage).getOrElse("<no message>")})",
        _.toString
      )
      val logs = Try(Option(container.getLogs).getOrElse("")).fold(
        error => s"<unavailable: ${Option(error.getMessage).getOrElse("<no message>")}>",
        identity
      )
      new TimeoutException(
        s"KurrentDB did not become ready within $timeout. url=$url, lastProbe=$probeDetail, " +
          s"containerRunning=$running\n--- container logs (last 8000 chars) ---\n${logs.takeRight(8000)}"
      )
    }

object KdbNode:

  final private val grpcPort = 2113
  final private val image    = sys.env.getOrElse("KDB_IMAGE", "docker.io/kurrentplatform/kurrentdb:26.1")

  private[api] val maxStreams: Int =
    sys.env.get("SEC_FIT_MAX_STREAMS").fold(8) { raw =>
      raw.toIntOption.filter(_ >= 3).getOrElse {
        throw new IllegalArgumentException(s"SEC_FIT_MAX_STREAMS must be an integer >= 3, but was '$raw'.")
      }
    }

  final private class Kdb(img: DockerImageName, hostPort: Int) extends GenericContainer[Kdb](img):
    addFixedExposedPort(hostPort, grpcPort)

  val resource: Resource[IO, KdbNode] = Resource
    .make(IO.blocking {
      val socket = new java.net.ServerSocket(0)
      val hostPort =
        try socket.getLocalPort
        finally socket.close()
      val c = new Kdb(DockerImageName.parse(image), hostPort)
      c.withEnv("KURRENTDB_INSECURE", "true")
      c.withEnv("KURRENTDB_MEM_DB", "false")
      c.withEnv("Kestrel__Limits__Http2__MaxStreamsPerConnection", maxStreams.toString)
      c.setWaitStrategy(Wait.forHttp("/health/live").forPort(grpcPort).forStatusCode(204))
      c.start()
      c
    })(c => IO.blocking(c.stop()))
    .map(new KdbNode(_))

trait FSuite extends SecResourceSuite[KdbNode]:

  override def munitIOTimeout: Duration = 10.minutes

  def makeResource: Resource[IO, KdbNode] = KdbNode.resource

  final def node: KdbNode = resource

  /** Insecure single-node client against the container's mapped port. No gossip-based rerouting is involved for a
    * single node, so a random mapped port needs no advertise configuration on the server.
    */
  final def mkClient(mod: SingleNodeBuilder[IO] => SingleNodeBuilder[IO] = identity): Resource[IO, EsClient[IO]] =
    mod(
      EsClient
        .singleNode[IO](node.endpoint)
        .withChannelShutdownAwait(200.millis)
        .withLogger(log)
    ).resource

  final def genStreamId(streamPrefix: String): StreamId.Id = sampleOfGen(idGen.genStreamIdNormal(streamPrefix))

  final def genEvents(n: Int): NonEmptyList[EventData] =
    sampleOfGen(eventdataGen.eventDataNelN(n, eventTypeGen.defaultPrefix))
