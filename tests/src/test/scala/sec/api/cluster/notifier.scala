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
package cluster

import java.time.ZonedDateTime
import java.util.UUID
import java.{util => ju}

import cats.Applicative
import cats.data.{NonEmptyList => Nel, NonEmptySet => Nes}
import cats.effect._
import cats.effect.concurrent.Ref
import cats.effect.testing.specs2.CatsIO
import cats.syntax.all._
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.testing.TestingLogger
import org.specs2.mutable.Specification
import sec.arbitraries._

import VNodeState._
import Notifier._

class NotifierSpec extends Specification with CatsIO {

  import NotifierSpec._
  import cats.effect.IO.ioEffect // Dotty

  "Notifier" should {

    def mkId: UUID                                     = sampleOf[ju.UUID]
    def mkTs: ZonedDateTime                            = sampleOf[ZonedDateTime]
    def mkEp(addr: String, port: Int = 2113): Endpoint = Endpoint(addr, port)
    def mkCi(ms: MemberInfo*): ClusterInfo             = ClusterInfo(Set(ms: _*))

    ///

    "gossip" >> {

      "always notifies listener with seed when started" >> {

        val seed = Nes.of(mkEp("127.0.0.1"), mkEp("127.0.0.2"), mkEp("127.0.0.3"))

        val run = for {
          updates   <- mkUpdates[IO](Nil)
          halt      <- mkHaltR[IO]
          listener  <- mkListenerR[IO]
          endpoints <- Resource.liftF(Ref.of[IO, Nes[Endpoint]](seed))
          notifier   = gossip.create[IO](seed, (_, nes) => nes, updates, endpoints, halt)
          n         <- notifier.start(listener)
        } yield n *> listener.recordings.get

        run.use(_.map(_ shouldEqual List(seed.toNonEmptyList)))

      }

      "only notifies listener if update is different than last" >> {

        val m1 = MemberInfo(mkId, mkTs, Leader, isAlive = true, mkEp("127.0.0.1"))
        val m2 = MemberInfo(mkId, mkTs, Follower, isAlive = true, mkEp("127.0.0.2"))
        val m3 = MemberInfo(mkId, mkTs, Follower, isAlive = false, mkEp("127.0.0.3"))
        val m4 = MemberInfo(mkId, mkTs, Leader, isAlive = true, mkEp("127.0.0.2"))
        val m5 = MemberInfo(mkId, mkTs, Follower, isAlive = true, mkEp("127.0.0.3"))

        val seed = Nel.of(m1, m2, m3).map(_.httpEndpoint).toNes

        val next: (ClusterInfo, Nes[Endpoint]) => Nes[Endpoint] =
          (ci, s) => Nel.fromList(ci.members.toList.map(_.httpEndpoint)).map(_.toNes).getOrElse(s)

        val run = for {
          updates   <- mkUpdates[IO](List(mkCi(m1, m2, m3), mkCi(m4, m5), mkCi(m4, m5)))
          halt      <- mkHaltR[IO]
          listener  <- mkListenerR[IO]
          endpoints <- Resource.liftF(Ref.of[IO, Nes[Endpoint]](seed))
          notifier   = gossip.create[IO](seed, next, updates, endpoints, halt)
          n         <- notifier.start(listener)
        } yield n *> listener.recordings.get

        run.use(_.map(_ shouldEqual List(Nel.of(m1, m2, m3).map(_.httpEndpoint), Nel.of(m4, m5).map(_.httpEndpoint))))

      }

      "defaultSelector" >> {

        val m1 = MemberInfo(mkId, mkTs, Leader, isAlive = true, mkEp("127.0.0.1"))
        val m2 = MemberInfo(mkId, mkTs, Follower, isAlive = true, mkEp("127.0.0.2"))
        val m3 = MemberInfo(mkId, mkTs, Follower, isAlive = false, mkEp("127.0.0.3"))
        val m4 = MemberInfo(mkId, mkTs, Leader, isAlive = true, mkEp("127.0.0.2"))
        val m5 = MemberInfo(mkId, mkTs, Follower, isAlive = true, mkEp("127.0.0.3"))

        val seed: Nes[Endpoint] = Nel.of(m1, m2, m3).map(_.httpEndpoint).toNes

        gossip.defaultSelector(mkCi(m3, m4, m5), seed) shouldEqual Nes.of(m4.httpEndpoint, m5.httpEndpoint)
        gossip.defaultSelector(mkCi(m3), seed) shouldEqual seed
        gossip.defaultSelector(mkCi(), seed) shouldEqual seed

      }

    }

    "bestNodes" >> {

      "does not notify listener on empty list of members" >> {

        val run = for {
          updates  <- mkUpdates[IO](Nil)
          halt     <- mkHaltR[IO]
          listener <- mkListenerR[IO]
          notifier  = bestNodes.create[IO](NodePreference.Leader, (_, _) => IO(Nil), updates, halt)
          n        <- notifier.start(listener)
        } yield n *> listener.recordings.get

        run.use(_.map(_ shouldEqual Nil))

      }

      "notifies listener on non-empty list of members" >> {

        val m1 = MemberInfo(mkId, mkTs, Leader, isAlive = true, mkEp("127.0.0.2"))
        val m2 = MemberInfo(mkId, mkTs, Follower, isAlive = true, mkEp("127.0.0.3"))
        val m3 = MemberInfo(mkId, mkTs, Follower, isAlive = false, mkEp("127.0.0.1"))
        val ci = mkCi(m1, m2, m3)

        val run = for {
          listener <- mkListenerR[IO]
          updates  <- mkUpdates[IO](List(ci))
          halt     <- mkHaltR[IO]
          notifier  = bestNodes.create[IO](NodePreference.Leader, (ci, _) => IO(ci.members.toList), updates, halt)
          n        <- notifier.start(listener)
        } yield n *> listener.recordings.get

        run.use(_.map(_ shouldEqual List(Nel.of(m1.httpEndpoint, m2.httpEndpoint, m3.httpEndpoint))))

      }

      "defaultSelector" >> {

        val m1 = MemberInfo(mkId, mkTs, Leader, isAlive = true, mkEp("127.0.0.1"))
        val m2 = MemberInfo(mkId, mkTs, Follower, isAlive = true, mkEp("127.0.0.2"))
        val m3 = MemberInfo(mkId, mkTs, Follower, isAlive = false, mkEp("127.0.0.3"))
        val m4 = MemberInfo(mkId, mkTs, ReadOnlyReplica, isAlive = true, mkEp("127.0.0.4"))
        val m5 = MemberInfo(mkId, mkTs, PreLeader, isAlive = true, mkEp("127.0.0.5"))

        bestNodes.defaultSelector[IO](mkCi(m1, m2, m3), NodePreference.Leader).map(_ shouldEqual List(m1))
        bestNodes.defaultSelector[IO](mkCi(m1, m2, m3), NodePreference.Follower).map(_ shouldEqual List(m2, m3))
        bestNodes.defaultSelector[IO](mkCi(m2, m3, m4), NodePreference.ReadOnlyReplica).map(_ shouldEqual List(m4))
        bestNodes.defaultSelector[IO](mkCi(m5), NodePreference.Leader).map(_ shouldEqual Nil)

      }

    }

    "mkHaltSignal" >> {

      val log    = TestingLogger.impl[IO](debugEnabled = true)
      val signal = mkHaltSignal[IO](log).use(_ => IO.unit)
      val run    = signal *> log.logged

      run.map(_ shouldEqual Vector(TestingLogger.DEBUG("Notifier signalled to shutdown.", None)))

    }

  }

}

object NotifierSpec {

  final case class RecordingListener[F[_]](recordings: Ref[F, List[Nel[Endpoint]]]) extends Listener[F] {
    def onResult(result: Nel[Endpoint]): F[Unit] = recordings.update(_ :+ result)
  }

  def mkUpdates[F[_]: Applicative](
    updates: List[ClusterInfo]
  ): Resource[F, Stream[F, ClusterInfo]] =
    Resource.pure[F, Stream[F, ClusterInfo]](Stream.emits(updates))

  def mkHaltR[F[_]: Concurrent]: Resource[F, SignallingRef[F, Boolean]] =
    Resource.make(SignallingRef[F, Boolean](false))(_.set(true))

  def mkListenerR[F[_]: Sync]: Resource[F, RecordingListener[F]] = Resource.liftF(mkListener[F])
  def mkListener[F[_]: Sync]: F[RecordingListener[F]]            = Ref.of[F, List[Nel[Endpoint]]](Nil).map(RecordingListener[F])

}
