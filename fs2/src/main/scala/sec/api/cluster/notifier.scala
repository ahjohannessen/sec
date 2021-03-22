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

import scala.util.Random

import cats.data.{NonEmptyList => Nel, NonEmptySet => Nes}
import cats.effect._
import cats.effect.concurrent._
import cats.effect.implicits._
import cats.syntax.all._
import fs2.Stream
import fs2.concurrent.SignallingRef
import org.typelevel.log4cats.Logger

import Notifier.Listener
import NodePrioritizer.prioritizeNodes

private[sec] trait Notifier[F[_]] {
  def start(l: Listener[F]): Resource[F, F[Unit]]
}

private[sec] object Notifier {

  trait Listener[F[_]] {
    def onResult(result: Nel[Endpoint]): F[Unit]
  }

  ///

  object gossip {

    def apply[F[_]: Concurrent](
      seed: Nes[Endpoint],
      updates: Stream[F, ClusterInfo],
      log: Logger[F]
    ): Resource[F, Notifier[F]] = mkHaltSignal[F](log) >>= { halt =>
      Resource.eval(Ref[F].of(seed)).map(create(seed, defaultSelector, updates, _, halt))
    }

    def create[F[_]](
      seed: Nes[Endpoint],
      determineNext: (ClusterInfo, Nes[Endpoint]) => Nes[Endpoint],
      updates: Stream[F, ClusterInfo],
      endpoints: Ref[F, Nes[Endpoint]],
      halt: SignallingRef[F, Boolean]
    )(implicit F: Concurrent[F]): Notifier[F] = new Notifier[F] {

      def start(l: Listener[F]): Resource[F, F[Unit]] = {

        def update(ci: ClusterInfo): F[Unit] = endpoints.get >>= { current =>
          val next = determineNext(ci, seed)
          (endpoints.set(next) >> l.onResult(next.toNonEmptyList)).whenA(current =!= next)
        }

        val bootstrap = Resource.eval(l.onResult(seed.toNonEmptyList))
        val run       = updates.evalMap(update).interruptWhen(halt)

        bootstrap *> run.compile.drain.background
      }
    }

    def defaultSelector(ci: ClusterInfo, seed: Nes[Endpoint]): Nes[Endpoint] =
      ci.members.filter(_.isAlive).map(_.httpEndpoint).toList match {
        case x :: xs => Nes.of(x, xs: _*)
        case Nil     => seed
      }

  }

  ///

  object bestNodes {

    def apply[F[_]](
      np: NodePreference,
      updates: Stream[F, ClusterInfo],
      log: Logger[F]
    )(implicit F: Concurrent[F]): Resource[F, Notifier[F]] =
      mkHaltSignal[F](log).map(create(np, defaultSelector[F], updates, _))

    def create[F[_]](
      np: NodePreference,
      determineNext: (ClusterInfo, NodePreference) => F[List[MemberInfo]],
      updates: Stream[F, ClusterInfo],
      halt: SignallingRef[F, Boolean]
    )(implicit F: Concurrent[F]): Notifier[F] = new Notifier[F] {

      def start(l: Listener[F]): Resource[F, F[Unit]] = {

        def update(ci: ClusterInfo): F[Unit] = determineNext(ci, np) >>= {
          case Nil     => F.unit
          case x :: xs => l.onResult(Nel(x, xs).map(_.httpEndpoint))
        }

        updates.evalMap(update).interruptWhen(halt).compile.drain.background

      }
    }

    /*
     *  If first member prioritized is leader then we only return that member. Otherwise,
     *  we return first member and remaining members that are same kind. This allows us to
     *  piggy-back on round robin load balancer from grpc-java such that we can get multiple
     *  followers or readonly replicas when leader is not preferred.
     * */
    def defaultSelector[F[_]: Sync](ci: ClusterInfo, np: NodePreference): F[List[MemberInfo]] =
      prioritize[F](ci, np).map {
        case x :: _ if x.state.eqv(VNodeState.Leader) => x :: Nil
        case x :: xs                                  => x :: xs.filter(_.state.eqv(x.state))
        case Nil                                      => Nil
      }

    ///

    def prioritize[F[_]: Sync](ci: ClusterInfo, np: NodePreference): F[List[MemberInfo]] =
      Sync[F].delay(new Random()).map { rnd =>
        Nel.fromList(ci.members.toList).map(prioritizeNodes(_, np, rnd.nextLong())).getOrElse(List.empty)
      }

  }

  def mkHaltSignal[F[_]: Concurrent](log: Logger[F]): Resource[F, SignallingRef[F, Boolean]] =
    Resource.make(SignallingRef[F, Boolean](false))(sr => sr.set(true) *> log.debug("Notifier signalled to shutdown."))

}
