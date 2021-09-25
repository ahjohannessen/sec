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
import java.{util => ju}
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import cats.data.{NonEmptyList => Nel}
import cats.effect._
import cats.effect.testkit._
import cats.effect.testing.specs2.CatsEffect
import cats.syntax.all._
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.scalacheck.Gen
import org.specs2.specification.Retries
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import sec.api.exceptions.ServerUnavailable
import sec.arbitraries._

class ClusterWatchSpec extends Specification with Retries with TestInstances with CatsEffect {

  override def sleep: Duration = 500.millis

  import ClusterWatch.Cache

  "ClusterWatch" should {

    val mkLog: IO[Logger[IO]] = Slf4jLogger.fromName[IO]("cluster-watch-spec")

    "only emit changes in cluster info" >> {

      val options = ClusterOptions.default
        .withMaxDiscoverAttempts(1.some)
        .withNotificationInterval(150.millis)

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

      def readFn(ref: Ref[IO, Nel[ClusterInfo]]): IO[ClusterInfo] = ref
        .getAndUpdate {
          case Nel(_, t :: ts) => Nel(t, ts)
          case Nel(h, Nil)     => Nel.one(h)
        }
        .map(_.head)

      type Res = (Stream[IO, ClusterInfo], Ref[IO, List[ClusterInfo]])

      val result: Resource[IO, Res] = for {
        infos   <- Resource.eval(Ref.of[IO, Nel[ClusterInfo]](Nel.of(ci1, ci2, ci3, ci4, ci5)))
        store   <- Resource.eval(Ref.of[IO, List[ClusterInfo]](ci1 :: Nil))
        log     <- Resource.eval(mkLog)
        watch   <- ClusterWatch.create[IO](readFn(infos), options, recordingCache(store), log)
        changes <- Resource.eval(watch.subscribe.pure[IO])
      } yield (changes, store)

      val x = result.map { case (changes, store) =>
        changes.take(4).compile.toList.map(_ shouldEqual List(ci1, ci2, ci4, ci5)) >>
          store.get.map(_.lastOption shouldEqual ci5.some)
      }

      x.use(a => a)

    }

    "retry retriable errors until discovery attempts used" >> {

      val maxAttempts = 5

      val options = ClusterOptions.default
        .withMaxDiscoverAttempts(maxAttempts.some)
        .withRetryDelay(50.millis)
        .withRetryBackoffFactor(1)
        .withReadTimeout(50.millis)
        .withNotificationInterval(50.millis)

      def test(err: Throwable, count: Int): IO[MatchResult[Any]] = {

        val result = for {
          readCountRef <- Stream.eval(Ref.of[IO, Int](0))
          store        <- Stream.eval(Ref.of[IO, List[ClusterInfo]](Nil))
          readFn        = readCountRef.update(_ + 1) *> IO.raiseError(err) *> IO(ClusterInfo(Set.empty))
          log          <- Stream.eval(mkLog)
          _ <- Stream
                 .resource(ClusterWatch.create[IO](readFn, options, recordingCache(store), log))
                 .map(_ => -1)
                 .handleErrorWith(_ => Stream.eval(readCountRef.get))
          _     <- Stream.sleep[IO](500.millis)
          count <- Stream.eval(readCountRef.get)
        } yield count

        result.compile.lastOrError.map(_ shouldEqual count)

      }

      test(ServerUnavailable("Oh Noes"), 6) *>
        test(new TimeoutException("Oh Noes"), 6) *>
        test(new RuntimeException("Oh Noes"), 1)

    }

    "retry retriable error using retry delay for backoff" >> {

      val ec: TestContext     = TestContext()
      implicit val tc: Ticker = Ticker(ec)

      val options = ClusterOptions.default
        .withMaxDiscoverAttempts(2.some)
        .withRetryDelay(50.millis)
        .withRetryBackoffFactor(1)
        .withReadTimeout(50.millis)
        .withNotificationInterval(50.millis)

      val error = sampleOfGen(Gen.oneOf(ServerUnavailable("Oh Noes"), new TimeoutException("Oh Noes")))

      val countRef = Ref[IO].of(0).unsafeRunSync()(cats.effect.unsafe.implicits.global)
      val storeRef = Ref[IO].of(List.empty[ClusterInfo]).unsafeRunSync()(cats.effect.unsafe.implicits.global)
      val cache    = recordingCache(storeRef)
      val readFn   = countRef.update(_ + 1) *> IO.raiseError(error) *> IO(ClusterInfo(Set.empty))
      val log      = mkLog.unsafeRunSync()(cats.effect.unsafe.implicits.global)
      val watch    = ClusterWatch.create[IO](readFn, options, cache, log)

      def test(expected: Int) = {
        ec.tick(options.retryDelay)
        val valueF = countRef.get.unsafeToFuture()
        ec.tick()
        valueF.value.get.toOption.get shouldEqual expected
      }

      val _ = watch.allocated[ClusterWatch[IO]].unsafeRunAndForget()

      test(1)
      test(2)
      test(3)
      test(3) // retries used
    }

  }

  def recordingCache(ref: Ref[IO, List[ClusterInfo]]): Cache[IO] = new Cache[IO] {
    def set(ci: ClusterInfo): IO[Unit] = ref.update(_ :+ ci)
    def get: IO[ClusterInfo]           = ref.get.map(_.lastOption.getOrElse(ClusterInfo(Set.empty)))
  }

}
