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

import scala.concurrent.duration.*
import munit.*
import munit.catseffect.*
import cats.effect.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import sec.api.*
import sec.api.streams.Reads
import helpers.text.mkSnakeCase

trait SecSuite extends FunSuite:

  //

  def assertNot(
    cond: => Boolean,
    clue: => Any = "assertion failed"
  )(implicit loc: Location): Unit =
    assert(!cond, clue)

  //

  def group(name: String)(thunk: => Unit): Unit = {
    val countBefore     = munitTestsBuffer.size
    val _               = thunk
    val countAfter      = munitTestsBuffer.size
    val countRegistered = countAfter - countBefore
    val registered      = munitTestsBuffer.toList.drop(countBefore)
    (0 until countRegistered).foreach(_ => munitTestsBuffer.remove(countBefore))
    registered.foreach(t => munitTestsBuffer += t.withName(s"$name - ${t.name}"))
  }

trait SecDisciplineSuite extends DisciplineSuite with SecSuite
trait SecScalaCheckSuite extends ScalaCheckSuite with SecSuite
trait SecEffectSuite extends CatsEffectSuite with SecSuite

trait SecResourceSuite[A] extends SecEffectSuite:

  private lazy val testName = mkSnakeCase(getClass.getSimpleName)
  private lazy val logger   = Slf4jLogger.fromName[IO](testName).unsafeRunSync()

  private lazy val clientFixture: IOFixture[A] =
    ResourceSuiteLocalFixture(s"${testName}_fixture", makeResource)

  override def munitFixtures = List(clientFixture)

  protected def makeResource: Resource[IO, A]
  final protected def resource: A     = clientFixture()
  final protected def log: Logger[IO] = logger

abstract class ClientSuite extends SecResourceSuite[EsClient[IO]]:

  override def munitIOTimeout: Duration = 120.seconds

  final def client: EsClient[IO]         = resource
  final def streams: Streams[IO]         = client.streams
  final def reads: Reads[IO]             = client.streams.messageReads
  final def metaStreams: MetaStreams[IO] = client.metaStreams
  final def gossip: Gossip[IO]           = client.gossip
