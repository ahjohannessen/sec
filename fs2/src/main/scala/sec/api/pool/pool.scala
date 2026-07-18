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

  /** Held for one call's lifetime; permit released on finalize, including error and cancellation. Raises
    * `IllegalStateException` on a closed pool - a lease after close means the pool's resource scope was escaped.
    */
  def lease: Resource[F, A]

  /** Occupancy per slot, pull-based. */
  def stats: F[Vector[Int]]

private[sec] object Pool:

  private[pool] case class Entry[F[_], A](value: A, permits: Semaphore[F], close: F[Unit])

  private[pool] enum State[F[_], A]:
    case Open(entries: Vector[Entry[F, A]])
    case Closed[F[_], A]() extends State[F, A]

  private[pool] enum Outcome[F[_], A]:
    case Acquired(entry: Entry[F, A], grown: Option[GrowEvent])
    case Rejected[F[_], A](size: Int) extends Outcome[F, A]
    case Closed[F[_], A]() extends Outcome[F, A]

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
      .make(AtomicCell[F].of[State[F, A]](State.Open(Vector.empty))) { cell =>
        // Closing is a state transition, not a snapshot read: the cell is swapped to Closed under
        // the lock, so an acquire racing shutdown either commits first - and its slot is in the
        // snapshot closed below - or observes the closed pool and fails loudly. A slot can never
        // grow into the void after close. Closes run after the lock is released, in parallel, as
        // each may block for a shutdown-await period, and are attempted so one failed close
        // cannot strand the rest.
        cell
          .evalModify[Vector[Entry[F, A]]] {
            case State.Open(entries) => (State.Closed[F, A](), entries).pure[F]
            case c @ State.Closed()  => (c, Vector.empty).pure[F]
          }
          .flatMap(_.parTraverse_(_.close.attempt))
      }
      .map(state => new Impl(state, mk, healthy, cfg, onGrow))

  private class Impl[F[_], A](
    state: AtomicCell[F, State[F, A]],
    mk: Resource[F, A],
    healthy: A => F[Boolean],
    cfg: PoolConfig,
    onGrow: GrowEvent => F[Unit]
  )(using F: Concurrent[F])
    extends Pool[F, A]:

    def stats: F[Vector[Int]] =
      state.get.flatMap {
        case State.Open(entries) =>
          entries.traverse(_.permits.available.map(free => cfg.streamsPerChannel - free.toInt))
        case State.Closed() =>
          Vector.empty[Int].pure[F]
      }

    // The acquire is masked end-to-end. AtomicCell.evalModify runs its body cancelably (only the
    // mutex release is guaranteed) and Resource.make polls its acquire, so an unmasked acquire has
    // windows where cancellation lands after a permit is taken - or a slot is built - but before
    // the entry is committed and the release finalizer is registered, silently leaking capacity.
    // The runtime observes cancellation between any two unmasked stages (every
    // cancelationCheckThreshold run-loop iterations), so the windows are narrow but real.
    // Masking is safe because everything under the cell's lock is non-blocking by the invariant
    // documented on [[of]]; the cost is that a canceler waits out a short, bounded acquire.
    def lease: Resource[F, A] =
      Resource.make(F.uncancelable(_ => acquireEntry))(_.permits.release).map(_.value)

    // Runs fully masked, see lease. Everything inside evalModify runs while holding the cell's
    // lock: only non-blocking permit tryAcquire, pure policy and lazy construction happen there.
    // The decision is returned as a value; logging and error raising happen after the lock is
    // released - still inside the mask, so an acquired entry always reaches Resource.make.
    private def acquireEntry: F[Entry[F, A]] =
      state
        .evalModify[Outcome[F, A]] {
          case c @ State.Closed()  => (c, Outcome.Closed[F, A]()).pure[F]
          case State.Open(entries) =>
            views(entries).flatMap { vs =>
              val placed = PoolPolicy.placementOrder(vs).findM(i => entries(i).permits.tryAcquire).map(_.map(entries))
              placed.flatMap {
                case Some(e) => (State.Open(entries), Outcome.Acquired(e, grown = None)).pure[F]
                case None    =>
                  PoolPolicy.onSaturated(vs, cfg.streamsPerChannel, cfg.limit) match
                    case Saturated.Reject(n) =>
                      (State.Open(entries), Outcome.Rejected[F, A](n)).pure[F]
                    case Saturated.Grow(event) =>
                      mkEntry.map(e => (State.Open(entries :+ e), Outcome.Acquired(e, event.some)))
              }
            }
        }
        .flatMap {
          case Outcome.Acquired(e, grown) => grown.traverse_(onGrow).as(e)
          case Outcome.Rejected(n)        => exceptions.SubscriptionPoolExhausted(n).raiseError
          case Outcome.Closed()           =>
            new IllegalStateException("Pool is closed - lease attempted after its resource was finalized.").raiseError
        }

    private def views(entries: Vector[Entry[F, A]]): F[Vector[SlotView]] =
      entries.zipWithIndex.traverse { case (e, i) =>
        (e.permits.available, healthy(e.value)).mapN((free, h) => SlotView(i, free.toInt, h))
      }

    // The only blocking `.acquire` in the pool, and it never actually waits: the semaphore is
    // freshly created with all permits available. Everything else uses non-blocking tryAcquire,
    // which is what makes holding the cell's lock around this code safe. Runs inside the acquire
    // mask (see lease); the mask must reach past the AtomicCell commit for construction to be
    // safe, so it lives on lease rather than here - a slot built here is either committed and
    // closable, or never built.
    private def mkEntry: F[Entry[F, A]] =
      for
        ac  <- mk.allocated
        sem <- Semaphore[F](cfg.streamsPerChannel.toLong)
        _   <- sem.acquire
      yield Entry(ac._1, sem, ac._2)
