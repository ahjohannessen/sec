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

import scala.concurrent.duration.*
import cats.effect.{Deferred, IO, Resource}
import cats.effect.std.Supervisor
import cats.syntax.all.*
import sec.syntax.all.*
import sec.api.exceptions.ResubscriptionRequired
import sec.api.pool.{Limit, PoolConfig}

/** Fault-injection coverage against a real node: the failure modes the pool exists for, which unit tests and a
  * healthy integration node cannot provoke.
  */
class PoolFaultsSuite extends FSuite:

  import StreamState.*

  /** The value configured on the Kestrel server by [[KdbNode]]. */
  final private val maxStreams  = KdbNode.maxStreams
  final private val subscribers = maxStreams + 5

  test("beyond MAX_CONCURRENT_STREAMS: unpooled subscriptions stall loudly, pooled all confirm") {

    // The original production incident: grpc-java queues streams beyond the server's limit without
    // surfacing anything. Client-side, the confirmation timeout converts the silent stall into
    // ResubscriptionRequired; with retries disabled it surfaces here. The pool removes the stall.
    val confirmIn = 3.seconds
    val window    = confirmIn + 2.seconds

    def outcomes(client: EsClient[IO], prefix: String): IO[List[Either[Throwable, Unit]]] =
      (0 until subscribers).toList.parTraverse { i =>
        client.streams
          .subscribeToStream(genStreamId(s"${prefix}_${i}_"), None)
          .interruptAfter(window)
          .compile
          .drain
          .attempt
      }

    val unpooled =
      mkClient(_.withOperationsRetryDisabled.withSubscriptionConfirmationTimeout(confirmIn))

    val pooled = Resource
      .eval(PoolConfig.of[IO](maxStreams, Limit.Bounded(3)))
      .flatMap { pc =>
        mkClient(_.withSubscriptionPool(pc).withOperationsRetryDisabled.withSubscriptionConfirmationTimeout(confirmIn))
      }

    for
      u      <- unpooled.use(outcomes(_, "fit_msc_unpooled"))
      stalled = u.count {
                  case Left(_: ResubscriptionRequired) => true
                  case _                               => false
                }
      other   = u.collect { case Left(t) if !t.isInstanceOf[ResubscriptionRequired] => t }
      _      <- IO(assert(other.isEmpty, s"unexpected failures: $other"))
      _      <- IO(assertEquals(
                  stalled,
                  subscribers - maxStreams,
                  s"expected exactly ${subscribers - maxStreams} subscriptions stalled beyond the configured " +
                    s"HTTP/2 stream limit of $maxStreams"
                ))
      p      <- pooled.use(outcomes(_, "fit_msc_pooled"))
      _      <- IO(assert(p.forall(_.isRight), s"pooled subscriptions failed: ${p.collect { case Left(t) => t }}"))
    yield ()
  }

  test("node restart: every pooled subscription resumes; a GOAWAY storm re-places instead of stalling") {

    val pooled = Resource
      .eval(PoolConfig.of[IO](2, Limit.Bounded(5)))
      .flatMap { pc =>
        mkClient(
          _.withSubscriptionPool(pc)
            .withOperationsRetryMaxAttempts(120)
            .withOperationsRetryMaxDelay(250.millis)
        )
      }

    pooled.use { client =>
      val ids = (0 until 6).toList.map(i => genStreamId(s"fit_restart_${i}_"))

      // Child fibers are finalized before pooled.use releases the client and its pool. If restart fails,
      // no resubscription fiber can escape the resource scope and attempt a lease on a closed pool.
      Supervisor[IO].use { supervisor =>
        for
          firstSeen <- ids.traverse(_ => Deferred[IO, Unit])
          subs      <- ids.zip(firstSeen).parTraverse { case (id, seen) =>
                         supervisor.supervise(
                           client.streams
                             .subscribeToStream(id, None)
                             .evalTap(_ => seen.complete(()).void)
                             .take(2)
                             .compile
                             .toList
                         )
                       }
          first     <- ids.parTraverse(id => client.streams.appendToStream(id, NoStream, genEvents(1)))
          _         <- firstSeen.parTraverse_(_.get).timeout(15.seconds)
          _         <- node.restart
          second    <- ids.parTraverse(id => client.streams.appendToStream(id, StreamState.Any, genEvents(1)))
          recs      <- subs.parTraverse(_.joinWithNever).timeout(45.seconds)
          expected  = first.zip(second).map { case (before, after) =>
                         List(before.streamPosition, after.streamPosition)
                       }
          _         <- IO(assertEquals(recs.map(_.map(event => Event.streamPosition(event))), expected))
        yield ()
      }
    }
  }
