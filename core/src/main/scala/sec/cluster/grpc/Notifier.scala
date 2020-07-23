package sec
package cluster
package grpc

import scala.jdk.CollectionConverters._
import io.grpc.NameResolver.ResolutionResult
import cats.data.{NonEmptyList => Nel, NonEmptySet => Nes}
import cats.implicits._
import cats.effect._
import cats.effect.concurrent._
import cats.effect.implicits._
import fs2.Stream
import fs2.concurrent.SignallingRef
import sec.api.Gossip._
import sec.api.Gossip.VNodeState._

trait Notifier[F[_]] {
  def start(l: Listener[F]): F[Unit]
}

object Notifier {

  object gossip {

    def apply[F[_]: Concurrent](
      seed: Nes[Endpoint],
      updates: Stream[F, ClusterInfo]
    ): F[Notifier[F]] = {
      for {
        halt      <- SignallingRef[F, Boolean](false)
        endpoints <- Ref[F].of(seed)
      } yield create(seed, defaultSelector, updates, endpoints, halt)
    }

    def create[F[_]](
      seed: Nes[Endpoint],
      determineNext: (ClusterInfo, Nes[Endpoint]) => Nes[Endpoint],
      updates: Stream[F, ClusterInfo],
      endpoints: Ref[F, Nes[Endpoint]],
      halt: SignallingRef[F, Boolean]
    )(implicit F: Concurrent[F]): Notifier[F] = (l: Listener[F]) => {

      val bootstrap = l.onResult(mkResult(seed))

      def update(ci: ClusterInfo): F[Unit] =
        endpoints.get.flatMap { current =>
          val next = determineNext(ci, seed)
          (endpoints.set(next) >> l.onResult(mkResult(next))).whenA(current =!= next)
        }

      val run = updates.evalMap(update).interruptWhen(halt)

      bootstrap *> run.compile.drain.start.void

    }

    def defaultSelector(ci: ClusterInfo, seed: Nes[Endpoint]): Nes[Endpoint] =
      ci.members.filter(_.isAlive).map(_.httpEndpoint).toList match {
        case x :: xs => Nes.of(x, xs: _*)
        case Nil     => seed
      }

    def mkResult(endpoints: Nes[Endpoint]): ResolutionResult =
      ResolutionResult.newBuilder().setAddresses(endpoints.toList.map(_.toEquivalentAddressGroup).asJava).build()

  }

  ///

  object bestNodes {

    def apply[F[_]: Concurrent](np: NodePreference, updates: Stream[F, ClusterInfo]): F[Notifier[F]] =
      SignallingRef[F, Boolean](false).map(create(np, defaultSelector[F], updates, _))

    def create[F[_]](
      np: NodePreference,
      determineNext: (ClusterInfo, NodePreference) => F[List[MemberInfo]],
      updates: Stream[F, ClusterInfo],
      halt: SignallingRef[F, Boolean]
    )(implicit F: Concurrent[F]): Notifier[F] = (l: Listener[F]) => {

      def update(ci: ClusterInfo): F[Unit] =
        determineNext(ci, np) >>= {
          case Nil     => F.unit
          case x :: xs => l.onResult(mkResult(x :: xs))
        }

      val run = updates
        .evalMap(update)
        .interruptWhen(halt)

      run.compile.drain.start.void
    }

    /*
     *  If first member prioritized is leader then we only return that member. Otherwise,
     *  we return first member and remaining members that are same kind. This allows us to
     *  piggy-back on round robin load balancer from grpc-java such that we can get multiple
     *  followers or readonly replicas when leader is not preferred.
     * */
    def defaultSelector[F[_]: Sync](ci: ClusterInfo, np: NodePreference): F[List[MemberInfo]] =
      prioritizeNodes[F](ci, np).map {
        case x :: _ if x.state === VNodeState.Leader => x :: Nil
        case x :: xs                                 => x :: xs.filter(_.state === x.state)
        case Nil                                     => Nil
      }

    ///

    def prioritizeNodes[F[_]: Sync](ci: ClusterInfo, np: NodePreference): F[List[MemberInfo]] =
      Nel.fromList(ci.members.toList).map(NodePrioritizer.prioritizeNodes[F](_, np)).getOrElse(List.empty.pure[F])

    def mkResult(ms: List[MemberInfo]): ResolutionResult =
      ResolutionResult.newBuilder().setAddresses(ms.map(_.httpEndpoint.toEquivalentAddressGroup).asJava).build()

  }

}
