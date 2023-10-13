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

import cats.*
import cats.data.{NonEmptyList as Nel, NonEmptySet as Nes}
import cats.effect.*
import cats.effect.implicits.*
import cats.effect.std.Random
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.Logger
import Notifier.Listener
import NodePrioritizer.prioritizeNodes

private[sec] trait Notifier[F[_]]:
  def start(l: Listener[F]): Resource[F, F[Unit]]

private[sec] object Notifier:

  trait Listener[F[_]] {
    def onResult(result: Nel[Endpoint]): F[Unit]
  }

  //

  object gossip:

    trait ChangeObserver[F[_]] {
      def observe(before: Nes[Endpoint], after: Nes[Endpoint]): F[Unit]
    }

    object ChangeObserver:

      def noop[F[_]: Applicative]: ChangeObserver[F] = new ChangeObserver[F]:
        def observe(before: Nes[Endpoint], after: Nes[Endpoint]): F[Unit] =
          Applicative[F].unit

      def logging[F[_]](log: Logger[F]): ChangeObserver[F] = new ChangeObserver[F]:
        def observe(before: Nes[Endpoint], after: Nes[Endpoint]): F[Unit] =
          log.info(s"Node endpoints changed ${renderEndpoints(before)} -> ${renderEndpoints(after)}")

    def renderEndpoints(eps: Nes[Endpoint]): String =
      eps.mkString_("[", ", ", "]")

    def mkStartupMsg(seed: Nes[Endpoint]): String =
      s"Starting notifier using seed ${renderEndpoints(seed)}"

    def apply[F[_]: Concurrent](
      seed: Nes[Endpoint],
      updates: Stream[F, ClusterInfo],
      log: Logger[F]
    ): Resource[F, Notifier[F]] =
      log.info(mkStartupMsg(seed)).toResource *> mkHaltSignal[F](log) >>= { halt =>
        Resource.eval(Ref[F].of(seed)).map(create(seed, defaultSelector, updates, _, halt, ChangeObserver.logging(log)))
      }

    def create[F[_]: Concurrent](
      seed: Nes[Endpoint],
      determineNext: (ClusterInfo, Nes[Endpoint]) => Nes[Endpoint],
      updates: Stream[F, ClusterInfo],
      endpoints: Ref[F, Nes[Endpoint]],
      halt: SignallingRef[F, Boolean],
      observer: ChangeObserver[F]
    ): Notifier[F] = (l: Listener[F]) =>

      def update(ci: ClusterInfo): F[Unit] =
        endpoints.get >>= { before =>
          val after  = determineNext(ci, seed)
          val change = endpoints.set(after) *> observer.observe(before, after) *> l.onResult(after.toNonEmptyList)
          change.whenA(before =!= after)
        }

      val bootstrap = Resource.eval(l.onResult(seed.toNonEmptyList))
      val run       = updates.evalMap(update).interruptWhen(halt)

      bootstrap *> run.compile.drain.background.map(_.void)

    def defaultSelector(ci: ClusterInfo, seed: Nes[Endpoint]): Nes[Endpoint] =
      ci.members.filter(_.isAlive).map(_.httpEndpoint).toList match
        case x :: xs => Nes.of(x, xs: _*)
        case Nil     => seed

  //

  object bestNodes:

    def mkStartupMsg(np: NodePreference): String =
      s"Starting notifier using node preference $np"

    def apply[F[_]: Async](
      np: NodePreference,
      updates: Stream[F, ClusterInfo],
      log: Logger[F]
    ): Resource[F, Notifier[F]] =
      log.info(mkStartupMsg(np)).toResource *>
        mkHaltSignal[F](log).map(create(np, loggingSelector[F](log)(defaultSelector[F]), updates, _))

    def create[F[_]: Concurrent](
      np: NodePreference,
      determineNext: (ClusterInfo, NodePreference) => F[List[MemberInfo]],
      updates: Stream[F, ClusterInfo],
      halt: SignallingRef[F, Boolean]
    ): Notifier[F] = (l: Listener[F]) =>

      def update(ci: ClusterInfo): F[Unit] = determineNext(ci, np) >>= {
        case Nil     => ().pure[F]
        case x :: xs => l.onResult(Nel(x, xs).map(_.httpEndpoint))
      }

      updates.changes.evalMap(update).interruptWhen(halt).compile.drain.background.map(_.void)

    def loggingSelector[F[_]: FlatMap](log: Logger[F])(
      inner: (ClusterInfo, NodePreference) => F[List[MemberInfo]]
    ): (ClusterInfo, NodePreference) => F[List[MemberInfo]] =
      (ci, np) =>
        inner(ci, np).flatTap { ms =>

          def render(mi: MemberInfo): String = {
            val endpoint = mi.httpEndpoint.render
            val state    = mi.state.render
            val status   = s"${if (mi.isAlive) "Alive" else "Dead"}"
            s"$state/$endpoint/$status"
          }

          val nodeSelection  = if (ms.isEmpty) "nothing" else ms.map(render).mkString(", ")
          val prefering      = if (ms.isEmpty) "" else s" prefering $np"
          val clusterMembers = ci.members.map(render).mkString(", ")

          log.info(s"Selected $nodeSelection from [$clusterMembers]$prefering")

        }

    /*
     *  If first member prioritized is leader then we only return that member. Otherwise,
     *  we return first member and remaining members that are same kind. This allows us to
     *  piggy-back on round robin load balancer from grpc-java such that we can get multiple
     *  followers or readonly replicas when leader is not preferred or temporarily missing.
     * */
    def defaultSelector[F[_]: Sync](ci: ClusterInfo, np: NodePreference): F[List[MemberInfo]] =
      prioritize[F](ci, np).map {
        case x :: _ if x.state.eqv(VNodeState.Leader) => x :: Nil
        case x :: xs                                  => x :: xs.filter(_.state.eqv(x.state))
        case Nil                                      => Nil
      }

    //

    def prioritize[F[_]: Sync](ci: ClusterInfo, np: NodePreference): F[List[MemberInfo]] =
      Random.scalaUtilRandom[F].flatMap(_.nextLong).map { nl =>
        Nel.fromList(ci.members.toList).map(prioritizeNodes(_, np, nl)).getOrElse(List.empty)
      }

  def mkHaltSignal[F[_]: Concurrent](log: Logger[F]): Resource[F, SignallingRef[F, Boolean]] =
    Resource.make(SignallingRef[F, Boolean](false))(sr => sr.set(true) *> log.debug("Notifier signalled to shutdown."))
