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
package cluster

import java.time.ZonedDateTime
import java.util as ju
import scala.concurrent.TimeoutException
import scala.concurrent.duration.*
import cats.Id
import cats.data.NonEmptySet as Nes
import cats.data.NonEmptyList as Nel
import cats.effect.*
import cats.effect.testkit.*
import cats.syntax.all.*
import fs2.Stream
import com.comcast.ip4s.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.scalacheck.Gen
import sec.api.exceptions.ServerUnavailable
import sec.arbitraries.*

class ClusterWatchSuite extends SecEffectSuite with TestInstances:

  import ClusterWatch.Cache

  val mkLog: IO[Logger[IO]] = Slf4jLogger.fromName[IO]("cluster-watch-spec")

  group("ClusterWatch") {

    test("only emit changes in cluster info") {

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
        assertIO(changes.take(4).compile.toList, List(ci1, ci2, ci4, ci5)) >>
          assertIO(store.get.map(_.lastOption), ci5.some)
      }

      x.use(a => a)

    }

    test("retry retriable errors until discovery attempts used") {

      val maxAttempts = 5

      val options = ClusterOptions.default
        .withMaxDiscoverAttempts(maxAttempts.some)
        .withRetryDelay(50.millis)
        .withRetryBackoffFactor(1)
        .withReadTimeout(50.millis)
        .withNotificationInterval(50.millis)

      def test(err: Throwable, count: Int): IO[Unit] =

        val result = for {
          readCountRef <- Stream.eval(Ref.of[IO, Int](0))
          store        <- Stream.eval(Ref.of[IO, List[ClusterInfo]](Nil))
          readFn        = readCountRef.update(_ + 1) *> IO.raiseError(err) *> IO(ClusterInfo(Set.empty))
          log          <- Stream.eval(mkLog)
          _            <- Stream
                 .resource(ClusterWatch.create[IO](readFn, options, recordingCache(store), log))
                 .map(_ => -1)
                 .handleErrorWith(_ => Stream.eval(readCountRef.get))
          _     <- Stream.sleep[IO](500.millis)
          count <- Stream.eval(readCountRef.get)
        } yield count

        assertIO(result.compile.lastOrError, count)

      test(ServerUnavailable("Oh Noes"), 6) *>
        test(new TimeoutException("Oh Noes"), 6) *>
        test(new RuntimeException("Oh Noes"), 1)

    }

    test("retry retriable error using retry delay for backoff") {

      val options = ClusterOptions.default
        .withMaxDiscoverAttempts(2.some)
        .withRetryDelay(50.millis)
        .withRetryBackoffFactor(1)
        .withReadTimeout(50.millis)
        .withNotificationInterval(50.millis)

      val error = sampleOfGen(Gen.oneOf(ServerUnavailable("Oh Noes"), new TimeoutException("Oh Noes")))

      val program: Resource[IO, Ref[IO, Int]] = for {
        counterRef <- Resource.eval(IO.ref(0))
        storeRef   <- Resource.eval(IO.ref(List.empty[ClusterInfo]))
        cache       = recordingCache(storeRef)
        readFn      = counterRef.update(_ + 1) *> IO.raiseError(error) *> IO.pure(ClusterInfo(Set.empty))
        log        <- Resource.eval(mkLog)
        _          <- ClusterWatch.create[IO](readFn, options, cache, log)
      } yield counterRef

      TestControl.execute(program.allocated) >>= { control =>

        def assertCount(
          outcome: Option[Outcome[Id, Throwable, (Ref[IO, Int], IO[Unit])]],
          expected: Int
        ): IO[Unit] =
          assertIO(succeededOrFail(outcome)._1.get, expected)

        for {
          _ <- control.tick

          _  <- control.advanceAndTick(options.retryDelay)
          r1 <- control.results
          _  <- assertCount(r1, 1)

          _  <- control.advanceAndTick(options.retryDelay)
          r2 <- control.results
          _  <- assertCount(r2, 2)

          _  <- control.advanceAndTick(options.retryDelay)
          r3 <- control.results
          _  <- assertCount(r3, 3)

          _  <- control.advanceAndTick(options.retryDelay)
          r4 <- control.results
          _  <- assertCount(r4, 3) // retries used

          shutdown <- succeededOrFail(r4)._2.attempt

        } yield shutdown

      }

    }
  }

  group("ClusterWatch.resolveSeed") {

    val maxAttempts    = 5
    val clusterOptions = ClusterOptions.default
      .withMaxDiscoverAttempts(maxAttempts.some)
      .withRetryDelay(150.millis)
      .withRetryBackoffFactor(1)
      .withReadTimeout(150.millis)

    val ep1 = Endpoint("127.0.0.1", 2113)
    val ep2 = Endpoint("127.0.0.2", 2113)
    val ep3 = Endpoint("127.0.0.3", 2113)

    def resolveEndpoints(clusterDns: Hostname, resolveFn: Hostname => IO[List[Endpoint]]) = mkLog >>= { l =>
      ClusterWatch.resolveEndpoints[IO](clusterDns, resolveFn, clusterOptions, l)
    }

    def resolveFn(ref: Ref[IO, Int], returnAfter: Int): Hostname => IO[List[Endpoint]] = hn =>
      hn.toString match {
        case "fail.org" => IO.raiseError(new RuntimeException("OhNoes"))
        case _          =>
          ref.updateAndGet(_ + 1) >>= { c =>
            if (returnAfter <= c) IO(List(ep1, ep2, ep3)) else IO(Nil)
          }
      }

    test("dns") {

      val program = for {
        ref       <- IO.ref(0)
        endpoints <- resolveEndpoints(host"example.org", resolveFn(ref, 1)).attempt
        count     <- ref.get
      } yield {
        assertEquals(count, 1)
        assertEquals(endpoints, Nes.of(ep1, ep2, ep3).asRight)
      }

      TestControl.executeEmbed(program)
    }

    test("dns - max attempts exhausted") {

      val program = for {
        ref       <- IO.ref(0)
        endpoints <- resolveEndpoints(host"example.org", resolveFn(ref, maxAttempts + 2)).attempt
        count     <- ref.get
      } yield {
        assertEquals(count, 6)
        assertEquals(endpoints, EndpointsResolveError(host"example.org").asLeft)
      }

      TestControl.executeEmbed(program)
    }

    test("dns - unknown errors are raised") {

      val program = for {
        ref       <- IO.ref(0)
        endpoints <- resolveEndpoints(host"fail.org", resolveFn(ref, 1)).attempt
        count     <- ref.get
      } yield {
        assertEquals(count, 0)
        assertEquals(endpoints.leftMap(_.getMessage()), "OhNoes".asLeft)
      }

      TestControl.executeEmbed(program)
    }

  }

  def succeededOrFail[A](outcome: Option[Outcome[Id, Throwable, A]]): A = outcome match
    case Some(_ @Outcome.Succeeded(a)) => a
    case other                         => fail(s"Expected Succeeded! Got $other")

  def recordingCache(ref: Ref[IO, List[ClusterInfo]]): Cache[IO] = new Cache[IO]:
    def set(ci: ClusterInfo): IO[Unit] = ref.update(_ :+ ci)
    def get: IO[ClusterInfo]           = ref.get.map(_.lastOption.getOrElse(ClusterInfo(Set.empty)))
