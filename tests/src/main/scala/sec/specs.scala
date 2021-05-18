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

import scala.concurrent.duration._
import cats.effect._
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import sec.api.{EsClient, Gossip, MetaStreams, Streams}
import helpers.text.mkSnakeCase

trait ResourceSpec[A] extends CatsEffectSuite {

  final val testName                        = mkSnakeCase(getClass.getSimpleName)
  final private lazy val logger: Logger[IO] = Slf4jLogger.fromName[IO](testName).unsafeRunSync()(ioRuntime)
  final protected def log: Logger[IO]       = logger

  protected def makeResource: Resource[IO, A]
  protected lazy val fixture: Fixture[A] = ResourceSuiteLocalFixture(testName, makeResource)
  override def munitFixtures             = List(fixture)

}

trait ClientSpec extends ResourceSpec[EsClient[IO]] {

  override val munitTimeout: Duration = 120.seconds

  final def client: EsClient[IO]         = fixture()
  final def streams: Streams[IO]         = client.streams
  final def metaStreams: MetaStreams[IO] = client.metaStreams
  final def gossip: Gossip[IO]           = client.gossip
}
