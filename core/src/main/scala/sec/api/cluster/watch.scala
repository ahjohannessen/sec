package sec
package api
package cluster

import scala.concurrent.duration._
import scala.concurrent.TimeoutException
import cats.data._
import cats.implicits._
import cats.effect._
import cats.effect.concurrent.Ref
import fs2.Stream
import io.chrisdavenport.log4cats.Logger
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import io.grpc._
import core.{NotLeader, ServerUnavailable}
import sec.api.Gossip.ClusterInfo
import sec.api.cluster.grpc._

private[sec] trait ClusterWatch[F[_]] {
  def subscribe: Stream[F, ClusterInfo]
}

private[sec] object ClusterWatch {

  def apply[F[_]: ConcurrentEffect: Timer, MCB <: ManagedChannelBuilder[MCB]](
    builderFromTarget: String => F[MCB],
    settings: Settings,
    gossipFn: ManagedChannel => Gossip[F],
    seed: NonEmptySet[Endpoint],
    authority: String,
    logger: Logger[F]
  ): Resource[F, ClusterWatch[F]] = {

    val mkCache: Resource[F, Cache[F]] = Resource.liftF(Cache(ClusterInfo(Set.empty)))

    def mkProvider(updates: Stream[F, ClusterInfo]): Resource[F, ResolverProvider[F]] =
      ResolverProvider
        .gossip(authority, seed, updates, logger)
        .evalTap(p => Sync[F].delay(NameResolverRegistry.getDefaultRegistry.register(p)))

    def mkChannel(p: ResolverProvider[F]): Resource[F, ManagedChannel] = Resource
      .liftF(builderFromTarget(s"${p.scheme}:///"))
      .flatMap(_.defaultLoadBalancingPolicy("round_robin").resource[F])

    for {
      store    <- mkCache
      updates   = mkWatch(store.get, settings.notificationInterval).subscribe
      provider <- mkProvider(updates)
      channel  <- mkChannel(provider)
      watch    <- create[F](gossipFn(channel).read(None), settings, store, logger)
    } yield watch

  }

  def create[F[_]: ConcurrentEffect: Timer](
    readFn: F[ClusterInfo],
    settings: Settings,
    store: Cache[F],
    log: Logger[F]
  ): Resource[F, ClusterWatch[F]] = {

    val watch   = mkWatch(store.get, settings.notificationInterval)
    val fetcher = mkFetcher(readFn, settings, store.set, log)
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
    setInfo: ClusterInfo => F[Unit],
    log: Logger[F]
  ): Stream[F, Unit] = {
    import settings._

    val delay       = retryDelay min readTimeout
    val nextDelay   = retryStrategy.nextDelay _
    val maxAttempts = maxDiscoverAttempts.getOrElse(Int.MaxValue)

    val action = retryF(readFn, "gossip", delay, nextDelay, maxAttempts, readTimeout.some, log) {
      case _: TimeoutException | _: ServerUnavailable | _: NotLeader => true
      case _                                                         => false
    }

    Stream.eval(action).metered(notificationInterval).repeat.changesBy(_.members).evalMap(setInfo)
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
