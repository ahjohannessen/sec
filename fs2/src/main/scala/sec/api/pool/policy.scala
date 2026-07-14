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

import cats.ApplicativeThrow
import cats.syntax.all.*

/** Growth limit for the subscription channel pool. */
enum Limit:

  /** Grow to at most `max` channels, then fail fast with [[sec.api.exceptions.SubscriptionPoolExhausted]]. */
  case Bounded(max: Int)

  /** Grow forever; log at error level for every growth past `sanityCap`. */
  case Unbounded(sanityCap: Int)

object Limit:

  /** Ten channels is an order of magnitude above a healthy subscription count for a single client at typical
    * streams-per-channel values - a pool that wants to grow past it indicates a subscription leak, not load.
    */
  val default: Limit = Bounded(max = 10)

/** Configuration for the subscription channel pool. */
sealed abstract case class PoolConfig(streamsPerChannel: Int, limit: Limit)

object PoolConfig:

  /** @param streamsPerChannel
    *   maximum concurrent gRPC streams per channel, must be greater than or equal to 1. Has no default on purpose: it
    *   must equal the server-side HTTP/2 `MAX_CONCURRENT_STREAMS` limit (Kestrel `MaxStreamsPerConnection`), and a
    *   silent default could quietly disagree with the server - which is the class of failure the pool exists to
    *   eliminate.
    * @param limit
    *   growth limit, see [[Limit]] and [[Limit.default]]. Its `max` / `sanityCap` must be greater than or equal to 1.
    */
  def apply(streamsPerChannel: Int, limit: Limit): Either[InvalidInput, PoolConfig] =

    val guardLimit: Either[InvalidInput, Limit] = limit match
      case Limit.Bounded(max) if max < 1 =>
        InvalidInput(s"limit max must be >= 1, it was $max.").asLeft
      case Limit.Unbounded(cap) if cap < 1 =>
        InvalidInput(s"limit sanityCap must be >= 1, it was $cap.").asLeft
      case valid => valid.asRight

    if streamsPerChannel < 1 then InvalidInput(s"streamsPerChannel must be >= 1, it was $streamsPerChannel.").asLeft
    else guardLimit.map(new PoolConfig(streamsPerChannel, _) {})

  /** As [[apply]], with the validation error lifted into `F`. */
  def of[F[_]: ApplicativeThrow](streamsPerChannel: Int, limit: Limit): F[PoolConfig] =
    apply(streamsPerChannel, limit).liftTo[F]

private[sec] case class SlotView(index: Int, free: Int, healthy: Boolean)

private[sec] enum GrowEvent:
  case Grew(newSize: Int, streamCapacity: Int)
  case GrewPastCap(newSize: Int, sanityCap: Int, occupancy: Vector[Int])

private[sec] enum Saturated:
  case Grow(event: GrowEvent)
  case Reject(size: Int)

private[sec] object PoolPolicy:

  /** Candidates best-first: healthy before unhealthy (GOAWAY-freed permits must not attract placement), then most free
    * permits. Unhealthy slots remain candidates: when everything is down we still place, never block.
    */
  def placementOrder(slots: Vector[SlotView]): List[Int] =
    slots.sortBy(v => (!v.healthy, -v.free)).map(_.index).toList

  /** Called only when no slot in placementOrder accepted a permit. */
  def onSaturated(slots: Vector[SlotView], streamsPerChannel: Int, limit: Limit): Saturated =
    val n = slots.size + 1
    limit match
      case Limit.Bounded(max) if slots.size >= max =>
        Saturated.Reject(slots.size)
      case Limit.Unbounded(cap) if n > cap =>
        Saturated.Grow(GrowEvent.GrewPastCap(n, cap, slots.map(v => streamsPerChannel - v.free)))
      case _ =>
        Saturated.Grow(GrowEvent.Grew(n, n * streamsPerChannel))
