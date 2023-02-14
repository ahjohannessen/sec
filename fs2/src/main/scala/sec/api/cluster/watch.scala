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

import scala.concurrent.duration._

import cats.data._
import cats.syntax.all._
import cats.effect._
import com.comcast.ip4s._
import fs2.Stream
import org.typelevel.log4cats.Logger
import io.grpc._
import sec.api.exceptions.{NotLeader, ServerUnavailable}
import sec.api.retries._
import sec.api.channel._

private[sec] trait ClusterWatch[F[_]] {
  def subscribe: Stream[F, ClusterInfo]
}

private[sec] object ClusterWatch {

  def resolveEndpoints[F[_]: Async](
    clusterDns: Hostname,
    resolveFn: Hostname => F[List[Endpoint]],
    clusterOptions: ClusterOptions,
    logger: Logger[F]
  ): F[NonEmptySet[Endpoint]] = {

    val log = logger.withModifiedString(s => s"dns-discovery > $s")

    def raiseResolveError =
      EndpointsResolveError(clusterDns).raiseError[F, NonEmptySet[Endpoint]]

    val action: F[NonEmptySet[Endpoint]] =
      resolveFn(clusterDns).map(NonEmptyList.fromList) >>= {
        _.map(_.toNes).fold(raiseResolveError)(_.pure[F])
      }

    retry(action, "resolve-endpoints", retryConfig(clusterOptions), log) {
      case _: EndpointsResolveError | _: Timeout => true
      case _                                     => false
    }

  }

  def apply[F[_]: Async, MCB <: ManagedChannelBuilder[MCB]](
    builderFromTarget: String => F[MCB],
    options: Options,
    clusterOptions: ClusterOptions,
    gossipFn: ManagedChannel => Resource[F, Gossip[F]],
    seed: NonEmptySet[Endpoint],
    authority: String,
    logger: Logger[F]
  ): Resource[F, ClusterWatch[F]] = {

    val mkCache: Resource[F, Cache[F]] = Resource.eval(Cache(ClusterInfo(Set.empty)))

    def mkProvider(updates: Stream[F, ClusterInfo]): Resource[F, ResolverProvider[F]] =
      ResolverProvider
        .gossip(authority, seed, updates, logger)
        .evalTap(p => Sync[F].delay(NameResolverRegistry.getDefaultRegistry.register(p)))

    def mkChannel(p: ResolverProvider[F]): Resource[F, ManagedChannel] = Resource
      .eval(builderFromTarget(s"${p.scheme}:///"))
      .flatMap(b => resource[F](b.defaultLoadBalancingPolicy("round_robin").build, options.channelShutdownAwait))

    for {
      store    <- mkCache
      updates   = mkWatch(store.get, clusterOptions.notificationInterval).subscribe
      provider <- mkProvider(updates)
      channel  <- mkChannel(provider)
      gossip   <- gossipFn(channel)
      watch    <- create[F](gossip.read, clusterOptions, store, logger)
    } yield watch

  }

  def create[F[_]: Async](
    readFn: F[ClusterInfo],
    options: ClusterOptions,
    store: Cache[F],
    log: Logger[F]
  ): Resource[F, ClusterWatch[F]] = {

    val watch   = mkWatch(store.get, options.notificationInterval)
    val fetcher = mkFetcher(readFn, options, store.set, log)
    val create  = Stream.emit(watch).concurrently(fetcher)

    create.compile.resource.lastOrError

  }

  def mkWatch[F[_]: Temporal](get: F[ClusterInfo], interval: FiniteDuration): ClusterWatch[F] =
    new ClusterWatch[F] {
      val subscribe: Stream[F, ClusterInfo] = Stream.eval(get).metered(interval).repeat.changesBy(_.members)
    }

  def mkFetcher[F[_]: Async](
    readFn: F[ClusterInfo],
    co: ClusterOptions,
    setInfo: ClusterInfo => F[Unit],
    log: Logger[F]
  ): Stream[F, Unit] = {
    import co._

    val action = retry(readFn, "gossip", retryConfig(co), log) {
      case _: Timeout | _: ServerUnavailable | _: NotLeader => true
      case _                                                => false
    }

    Stream.eval(action).metered(notificationInterval).repeat.changesBy(_.members).evalMap(setInfo)
  }

  ///

  def retryConfig(co: ClusterOptions): RetryConfig = {
    val timeout     = co.readTimeout.some
    val maxAttempts = co.maxDiscoverAttempts.getOrElse(Int.MaxValue)
    RetryConfig(co.retryDelay, co.retryMaxDelay, co.retryBackoffFactor, maxAttempts, timeout)
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

//

final private[sec] case class EndpointsResolveError(clusterDns: Hostname)
  extends RuntimeException(s"No endpoints returned from $clusterDns")
