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

import cats.effect._
import cats.effect.testing.specs2._
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll
import helpers.text.snakeCaseTransformation
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import sec.api.{Gossip, Streams}

trait ResourceSpec[A] extends Specification with AfterAll with CatsIO {
  final val testName                        = snakeCaseTransformation(getClass.getSimpleName)
  final private lazy val logger: Logger[IO] = Slf4jLogger.fromName[IO](testName).unsafeRunSync()
  final private lazy val (value, shutdown)  = makeResource.allocated[IO, A].unsafeRunSync()
  final override def afterAll(): Unit       = shutdown.unsafeRunSync()

  protected def makeResource: Resource[IO, A]
  final protected def resource: A     = value
  final protected def log: Logger[IO] = logger
}

trait ClientSpec extends ResourceSpec[EsClient[IO]] {
  final def client: EsClient[IO] = resource
  final def streams: Streams[IO] = client.streams
  final def gossip: Gossip[IO]   = client.gossip
}
