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
package pool

import cats.~>
import cats.syntax.all.*
import cats.effect.{Concurrent, Ref, Resource}
import cats.effect.syntax.all.*
import fs2.Stream
import fs2.grpc.client.{ClientAspectMiddleware, ClientCallContext}
import io.grpc.Metadata
import org.typelevel.log4cats.Logger

/** Counts in-flight RPCs on the regular (ops) channel when a subscription pool is configured. Pure observation: ops
  * queueing is transient and harmless for calls that end, but sustained saturation indicates pathology - e.g. a
  * hand-rolled infinite read that belongs on the pooled subscription transport. Warns with hysteresis at 90% of the
  * concurrent-stream limit and logs resumption at 75%.
  */
final private[sec] class CallCounter[F[_]] private (
  state: Ref[F, CallCounter.State],
  streamsPerChannel: Int,
  warnAt: Int,
  resumeAt: Int,
  log: Logger[F]
)(using F: Concurrent[F]):

  import CallCounter.State

  private def warn(n: Int): F[Unit] =
    log.warn(
      s"At $n in-flight calls, at or above $warnAt (90% of the concurrent-stream limit " +
        s"$streamsPerChannel). Sustained saturation delays reads and appends and may indicate long-lived " +
        "reads that belong on subscriptions."
    )

  private def resume(n: Int): F[Unit] =
    log.info(s"Back at $n in-flight calls, at or below $resumeAt (75% of the concurrent-stream limit).")

  private val incr: F[Unit] =
    state.modify { s =>
      val inFlight = s.inFlight + 1
      if !s.warned && inFlight >= warnAt then (State(inFlight, warned = true), warn(inFlight))
      else (s.copy(inFlight = inFlight), F.unit)
    }.flatten

  private val decr: F[Unit] =
    state.modify { s =>
      val inFlight = s.inFlight - 1
      if s.warned && inFlight <= resumeAt then (State(inFlight, warned = false), resume(inFlight))
      else (s.copy(inFlight = inFlight), F.unit)
    }.flatten

  // One place owns the increment/decrement pairing: decrement runs on finalize,
  // including error and cancellation, so the counter conserves on every path.
  private val counted: Resource[F, Unit] =
    Resource.make(incr)(_ => decr)

  /** Counted for the effect's lifetime. */
  def count[A](fa: F[A]): F[A] =
    counted.surround(fa)

  /** Counted for the stream's lifetime. */
  def countStream[A](s: Stream[F, A]): Stream[F, A] =
    Stream.resource(counted) >> s

  val middleware: ClientAspectMiddleware[F, Metadata] = new ClientAspectMiddleware[F, Metadata]:

    def unary[Req, Res](callCtx: ClientCallContext[Req, Res, Metadata]): F ~> F =
      new (F ~> F):
        def apply[A](fa: F[A]): F[A] = count(fa)

    def streaming[Req, Res](callCtx: ClientCallContext[Req, Res, Metadata]): Stream[F, *] ~> Stream[F, *] =
      new (Stream[F, *] ~> Stream[F, *]):
        def apply[A](sa: Stream[F, A]): Stream[F, A] = countStream(sa)

private[sec] object CallCounter:

  private[pool] case class State(inFlight: Int, warned: Boolean)

  def of[F[_]: Concurrent](streamsPerChannel: Int, log: Logger[F]): F[CallCounter[F]] =
    Ref.of[F, State](State(0, warned = false)).map { state =>
      new CallCounter(
        state,
        streamsPerChannel,
        warnAt   = math.max(1, streamsPerChannel * 9 / 10),
        resumeAt = streamsPerChannel * 3 / 4,
        log      = log
      )
    }
