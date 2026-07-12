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

import cats.effect.{Async, Resource, Sync}
import cats.syntax.all.*
import io.grpc.{ConnectivityState, ManagedChannel}
import io.kurrent.dbclient.proto.streams.StreamsFs2Grpc
import org.typelevel.log4cats.Logger

/** One pooled unit: the channel (health introspection + shutdown) plus the streams stub bound to it. */
private[sec] case class GrpcSlot[F[_]](
  channel: ManagedChannel,
  streams: StreamsFs2Grpc[F, Context]
)

/** What a builder supplies for constructing a subscription channel pool: its configuration and the recipe for
  * additional channels, sharing the credentials, authority and settings of the regular channel.
  */
private[sec] case class SubscriptionPool[F[_]](
  config: PoolConfig,
  mkChannel: Resource[F, ManagedChannel]
)

private[sec] object GrpcChannelPool:

  /** Slot construction runs while the pool's internal lock is held, so it must not block or perform I/O. That holds for
    * `mkChannel` and `mkStubs` as supplied by the builders and [[EsClient]]: `NettyChannelBuilder.build()` constructs
    * but does not connect (grpc-java dials on first RPC), and stub creation is `Dispatcher` allocation plus wiring -
    * fiber spawning, no network I/O. Credential material is resolved once by the builder, never per slot.
    *
    * The channel finalizer DOES block (`awaitTermination`), but it only runs in the pool's outer finalizer at shutdown
    * \- never under the pool lock.
    */
  def of[F[_]: Async](
    mkChannel: Resource[F, ManagedChannel],
    mkStubs: ManagedChannel => Resource[F, StreamsFs2Grpc[F, Context]],
    cfg: PoolConfig,
    logger: Logger[F]
  ): Resource[F, Pool[F, GrpcSlot[F]]] =

    val mkSlot: Resource[F, GrpcSlot[F]] =
      for
        mc <- mkChannel
        s  <- mkStubs(mc)
      yield GrpcSlot(mc, s)

    Pool.of(
      mk      = mkSlot,
      healthy = slotHealthy[F],
      cfg     = cfg,
      onGrow  = logGrow(logger)
    )

  /** `getState(false)` is a non-blocking volatile read - safe under the pool lock. `false` = do NOT trigger a
    * connection attempt; the pool observes state, never causes dials. IDLE/CONNECTING count as healthy: an undialed
    * channel is a legitimate target (grpc-java connects on first RPC). Only active failure or shutdown deprioritizes.
    */
  private def slotHealthy[F[_]: Sync](slot: GrpcSlot[F]): F[Boolean] =
    Sync[F].delay(slot.channel.getState(false)).map {
      case ConnectivityState.TRANSIENT_FAILURE | ConnectivityState.SHUTDOWN => false
      case _                                                                => true
    }

  private def logGrow[F[_]](log: Logger[F])(e: GrowEvent): F[Unit] = e match
    case GrowEvent.Grew(n, capacity) =>
      log.info(s"Growing to $n channels (~$capacity stream capacity).")
    case GrowEvent.GrewPastCap(n, cap, occupancy) =>
      log.error(
        s"At $n channels - PAST sanity cap $cap. " +
          s"Per-channel occupancy: ${occupancy.mkString("[", " ", "]")}. " +
          "Likely a subscription leak or runaway resubscribe loop. The pool will keep growing; investigate."
      )
