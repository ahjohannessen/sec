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
package cluster

import java.{util => ju}
import java.time.ZonedDateTime
import scala.concurrent.duration._
import scala.concurrent.TimeoutException
import org.specs2.mutable.Specification
import cats.syntax.all._
import cats.effect._
import cats.effect.concurrent.Ref
import cats.effect.testing.specs2.CatsIO
import fs2.Stream
import sec.core.ServerUnavailable
import sec.api.Gossip._
import Arbitraries._
import cats.effect.laws.util.TestContext
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.chrisdavenport.log4cats.Logger
import org.scalacheck.Gen

class ClusterWatchSpec extends Specification with CatsIO {

  import ClusterWatch.Cache

  "ClusterWatch" should {

    "only emit changes in cluster info" >> {

      val settings = ClusterSettings.default
        .withMaxDiscoverAttempts(1.some)
        .withNotificationInterval(50.millis)

      def instanceId = sampleOf[ju.UUID]
      def timestamp  = sampleOf[ZonedDateTime]

      val m1 = MemberInfo(instanceId, timestamp, VNodeState.Leader, isAlive = true, Endpoint("127.0.0.1", 2113))
      val m2 = MemberInfo(instanceId, timestamp, VNodeState.Leader, isAlive = true, Endpoint("127.0.0.2", 2113))
      val m3 = MemberInfo(instanceId, timestamp, VNodeState.Leader, isAlive = true, Endpoint("127.0.0.3", 2113))

      val ci1 = ClusterInfo(Set(m1))
      val ci2 = ClusterInfo(Set(m1, m2))
      val ci3 = ClusterInfo(Set(m2, m1))
      val ci4 = ClusterInfo(Set(m1, m2, m3))
      val ci5 = ClusterInfo(Set(m1, m3))

      def readFn(ref: Ref[IO, Int]): IO[ClusterInfo] =
        ref.get.flatMap {
          case 0 => ref.update(_ + 1) *> ci1.pure[IO]
          case 1 => ref.update(_ + 1) *> ci2.pure[IO]
          case 2 => ref.update(_ + 1) *> ci3.pure[IO]
          case 3 => ref.update(_ + 1) *> ci4.pure[IO]
          case _ => ref.update(_ + 1) *> ci5.pure[IO]
        }

      val result = for {
        readCountRef <- Resource.liftF(Ref.of[IO, Int](0))
        store        <- Resource.liftF(Ref.of[IO, List[ClusterInfo]](ci1 :: Nil))
        log          <- Resource.liftF(mkLog)
        watch        <- ClusterWatch.create[IO](readFn(readCountRef), settings, recordingCache(store), log)
        changes      <- Resource.liftF(watch.subscribe.pure[IO])
      } yield (changes, store)

      result.map {
        case (changes, store) =>
          changes.take(4).compile.toList.map(_ shouldEqual List(ci1, ci2, ci4, ci5)) *>
            store.get.map(_.lastOption shouldEqual ci5.some)
      }

    }

    "retry retriable errors until discovery attempts used" >> {

      val maxAttempts = 5

      val settings = ClusterSettings.default
        .withMaxDiscoverAttempts(maxAttempts.some)
        .withRetryDelay(50.millis)
        .withRetryBackoffFactor(1)
        .withReadTimeout(50.millis)
        .withNotificationInterval(50.millis)

      def test(err: Throwable, count: Int) = {

        val result = for {
          readCountRef <- Stream.eval(Ref.of[IO, Int](0))
          store        <- Stream.eval(Ref.of[IO, List[ClusterInfo]](Nil))
          readFn        = readCountRef.update(_ + 1) *> IO.raiseError(err) *> IO(ClusterInfo(Set.empty))
          log          <- Stream.eval(mkLog)
          _ <- Stream
                 .resource(ClusterWatch.create[IO](readFn, settings, recordingCache(store), log))
                 .map(_ => -1)
                 .handleErrorWith(_ => Stream.eval(readCountRef.get))
          _     <- Stream.sleep(500.millis)
          count <- Stream.eval(readCountRef.get)
        } yield count

        result.compile.lastOrError.map(_ shouldEqual count)

      }

      test(ServerUnavailable("Oh Noes"), maxAttempts) *>
        test(new TimeoutException("Oh Noes"), maxAttempts) *>
        test(new RuntimeException("Oh Noes"), 1)

    }

    "retry retriable error using retry delay for backoff" >> {

      val ec: TestContext          = TestContext()
      val ce: ConcurrentEffect[IO] = IO.ioConcurrentEffect(IO.contextShift(ec))

      val settings = ClusterSettings.default
        .withMaxDiscoverAttempts(3.some)
        .withRetryDelay(50.millis)
        .withRetryBackoffFactor(1)
        .withReadTimeout(50.millis)
        .withNotificationInterval(50.millis)

      val error = sampleOfGen(Gen.oneOf(ServerUnavailable("Oh Noes"), new TimeoutException("Oh Noes")))

      val countRef = Ref[IO].of(0).unsafeRunSync()
      val storeRef = Ref[IO].of(List.empty[ClusterInfo]).unsafeRunSync()
      val cache    = recordingCache(storeRef)
      val readFn   = countRef.update(_ + 1) *> IO.raiseError(error) *> IO(ClusterInfo(Set.empty))
      val log      = mkLog.unsafeRunSync()
      val watch    = ClusterWatch.create[IO](readFn, settings, cache, log)(ce, ec.timer)

      def test(expected: Int) = {
        ec.tick(settings.retryDelay)
        countRef.get.unsafeRunSync() shouldEqual expected
      }

      val _ = watch.allocated[IO, ClusterWatch[IO]].unsafeRunAsyncAndForget()

      test(1)
      test(2)
      test(3)
      test(3) // retries used
    }

  }

  def recordingCache(ref: Ref[IO, List[ClusterInfo]]): Cache[IO] =
    new Cache[IO] {
      def set(ci: ClusterInfo): IO[Unit] = ref.update(_ :+ ci)
      def get: IO[ClusterInfo]           = ref.get.map(_.lastOption.getOrElse(ClusterInfo(Set.empty)))
    }

  val mkLog: IO[Logger[IO]] = Slf4jLogger.fromName[IO]("cluster-watch-spec")

}
