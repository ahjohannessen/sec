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

import cats.syntax.all.*
import cats.effect.{Concurrent, Resource}
import cats.effect.implicits.*
import cats.effect.std.{AtomicCell, Semaphore}

/** A never-shrinking pool of `A` where every lease holds a semaphore permit for its lifetime, so the transport never
  * sees more concurrent calls than configured and grpc-java's silent stream queueing is unrepresentable.
  */
private[sec] trait Pool[F[_], A]:

  /** Held for one call's lifetime; permit released on finalize, including error and cancellation. */
  def lease: Resource[F, A]

  /** Occupancy per slot, pull-based. */
  def stats: F[Vector[Int]]

private[sec] object Pool:

  private[pool] case class Entry[F[_], A](value: A, permits: Semaphore[F], close: F[Unit])

  private[pool] enum Outcome[F[_], A]:
    case Acquired(entry: Entry[F, A], grown: Option[GrowEvent])
    case Rejected[F[_], A](size: Int) extends Outcome[F, A]

  /** The pool serializes placement and growth through a single lock (the [[AtomicCell]]), so nothing that runs while it
    * is held may block or perform I/O: `mk` must only construct lazily (e.g. build an undialed channel) and `healthy`
    * must be a non-blocking read. `onGrow` runs after the lock is released.
    */
  def of[F[_], A](
    mk: Resource[F, A],
    healthy: A => F[Boolean],
    cfg: PoolConfig,
    onGrow: GrowEvent => F[Unit]
  )(using F: Concurrent[F]): Resource[F, Pool[F, A]] =
    Resource
      .make(AtomicCell[F].of(Vector.empty[Entry[F, A]]))(
        // Closing the pool closes every slot. Closes run in parallel, as each one may block for
        // a shutdown-await period, and are attempted so one failed close cannot strand the rest.
        _.get.flatMap(_.parTraverse_(_.close.attempt))
      )
      .map(state => new Impl(state, mk, healthy, cfg, onGrow))

  private class Impl[F[_], A](
    state: AtomicCell[F, Vector[Entry[F, A]]],
    mk: Resource[F, A],
    healthy: A => F[Boolean],
    cfg: PoolConfig,
    onGrow: GrowEvent => F[Unit]
  )(using F: Concurrent[F])
    extends Pool[F, A]:

    def stats: F[Vector[Int]] =
      state.get.flatMap(_.traverse(_.permits.available.map(free => cfg.streamsPerChannel - free.toInt)))

    def lease: Resource[F, A] =
      Resource.make(acquireEntry)(_.permits.release).map(_.value)

    // Everything inside evalModify runs while holding the cell's lock: only non-blocking
    // permit tryAcquire, pure policy and lazy construction happen there. The decision is
    // returned as a value; logging and error raising happen after the lock is released.
    private def acquireEntry: F[Entry[F, A]] =
      state
        .evalModify { entries =>
          views(entries).flatMap { vs =>
            val placed = PoolPolicy.placementOrder(vs).findM(i => entries(i).permits.tryAcquire).map(_.map(entries))
            placed.flatMap {
              case Some(e) => (entries, Outcome.Acquired(e, grown = None)).pure[F]
              case None    =>
                PoolPolicy.onSaturated(vs, cfg.streamsPerChannel, cfg.limit) match
                  case Saturated.Reject(n) =>
                    (entries, Outcome.Rejected[F, A](n)).pure[F]
                  case Saturated.Grow(event) =>
                    mkEntry.map(e => (entries :+ e, Outcome.Acquired(e, event.some)))
            }
          }
        }
        .flatMap {
          case Outcome.Acquired(e, grown) => grown.traverse_(onGrow).as(e)
          case Outcome.Rejected(n)        => exceptions.SubscriptionPoolExhausted(n).raiseError
        }

    private def views(entries: Vector[Entry[F, A]]): F[Vector[SlotView]] =
      entries.zipWithIndex.traverse { case (e, i) =>
        (e.permits.available, healthy(e.value)).mapN((free, h) => SlotView(i, free.toInt, h))
      }

    // The only blocking `.acquire` in the pool, and it never actually waits: the semaphore is
    // freshly created with all permits available. Everything else uses non-blocking tryAcquire,
    // which is what makes holding the cell's lock around this code safe. uncancelable spans
    // construction-then-registration so a cancelled caller cannot leave behind a slot that was
    // built but never recorded (and would then never be closed).
    private def mkEntry: F[Entry[F, A]] =
      F.uncancelable { _ =>
        for
          ac  <- mk.allocated
          sem <- Semaphore[F](cfg.streamsPerChannel.toLong)
          _   <- sem.acquire
        yield Entry(ac._1, sem, ac._2)
      }
