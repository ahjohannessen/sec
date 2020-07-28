package sec
package cluster

import scala.concurrent.duration._
import scala.concurrent.TimeoutException
import cats.data._
import cats.implicits._
import cats.effect._
import cats.effect.concurrent.Ref
import fs2.Stream
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import sec.core.ServerUnavailable
import sec.api.Gossip
import sec.api.Gossip.{ClusterInfo, Endpoint}
import sec.cluster.grpc._

private[sec] trait ClusterWatch[F[_]] {
  def subscribe: Stream[F, ClusterInfo]
}

private[sec] object ClusterWatch {

  def apply[F[_]: ConcurrentEffect: Timer, MCB <: ManagedChannelBuilder[MCB]](
    builderFromTarget: String => MCB,
    settings: Settings,
    gossipFn: ManagedChannel => Gossip[F],
    seed: NonEmptySet[Endpoint],
    authority: String
  ): Resource[F, ClusterWatch[F]] = {

    val mkCache: Resource[F, Cache[F]] = Resource.liftF(Cache(ClusterInfo(Set.empty)))

    def mkChannel(updates: Stream[F, ClusterInfo]): Resource[F, ManagedChannel] =
      builderFromTarget(ResolverProvider.gossipScheme)
        .nameResolverFactory(ResolverProvider.gossip(authority, seed, updates))
        .defaultLoadBalancingPolicy("round_robin")
        .resource[F]

    for {
      store   <- mkCache
      initial  = mkWatch(store.get, settings.notificationInterval).subscribe
      channel <- mkChannel(initial)
      watch   <- create[F](gossipFn(channel).read(None), settings, store)
    } yield watch

  }

  def create[F[_]: ConcurrentEffect: Timer](
    readFn: F[ClusterInfo],
    settings: Settings,
    store: Cache[F]
  ): Resource[F, ClusterWatch[F]] = {

    val watch   = mkWatch(store.get, settings.notificationInterval)
    val fetcher = mkFetcher(readFn, settings, store.set)
    val create  = Stream.emit(watch).concurrently(fetcher)

    create.compile.resource.lastOrError

  }

  def mkWatch[F[_]: Sync: Timer](get: F[ClusterInfo], interval: FiniteDuration): ClusterWatch[F] =
    new ClusterWatch[F] {
      val subscribe: Stream[F, ClusterInfo] = Stream.eval(get).metered(interval).repeat.changesBy(_.members)
    }

  def mkFetcher[F[_]: ConcurrentEffect: Timer](
    readFn: F[ClusterInfo],
    settings: Settings,
    setInfo: ClusterInfo => F[Unit]
  ): Stream[F, Unit] = {

    import settings._

    val retriable: PartialFunction[Throwable, Boolean] = {
      case _: TimeoutException | _: ServerUnavailable => true
      case _                                          => false
    }

    val maxAttempts   = maxDiscoverAttempts.getOrElse(Int.MaxValue)
    val read          = Stream.eval(readFn).timeout(readTimeout).compile.lastOrError
    val readWithRetry = Stream.retry(read, retryDelay min readTimeout, identity, maxAttempts, retriable)

    readWithRetry.metered(notificationInterval).repeat.changesBy(_.members).evalMap(setInfo)
  }

  ///

  trait Cache[F[_]] {
    def set(ci: ClusterInfo): F[Unit]
    def get: F[ClusterInfo]
  }

  object Cache {

    def apply[F[_]: Sync](
      ci: ClusterInfo
    ): F[Cache[F]] = Ref[F].of(ci).map(create)

    def create[F[_]](ref: Ref[F, ClusterInfo]): Cache[F] =
      new Cache[F] {
        def set(ci: ClusterInfo): F[Unit] = ref.set(ci)
        def get: F[ClusterInfo]           = ref.get
      }
  }

}
