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
package tsc

import java.io.File
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.jdk.CollectionConverters._
import cats.{Applicative, Endo}
import cats.data.NonEmptySet
import cats.syntax.all._
import cats.effect._
import io.grpc.ManagedChannelBuilder
import com.typesafe.config._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import sec.api._

private[sec] object config {

  private val defaultPort    = 2113
  private val defaultAddress = "127.0.0.1"

  private val root   = "sec"
  private val snPath = s"$root.single-node"
  private val clPath = s"$root.cluster"
  private val ooPath = s"$root.operations"
  private val coPath = s"$clPath.options"

  ///

  def mkClient[F[_]: ConcurrentEffect: Timer, MCB <: ManagedChannelBuilder[MCB]](
    mcb: ChannelBuilderParams => F[MCB],
    cfg: Config
  ): Resource[F, EsClient[F]] = mkClient[F, MCB](mcb, cfg, NoOpLogger.impl[F])

  def mkClient[F[_]: ConcurrentEffect: Timer, MCB <: ManagedChannelBuilder[MCB]](
    mcb: ChannelBuilderParams => F[MCB],
    cfg: Config,
    logger: Logger[F]
  ): Resource[F, EsClient[F]] =
    Resource.eval(mkBuilder[F](cfg, logger)) >>= {
      case Left(c)  => c.build(mcb)
      case Right(s) => s.build(mcb)
    }

  ///

  def mkBuilder[F[_]: MonadThrow](
    cfg: Config,
    logger: Logger[F]
  ): F[Either[ClusterBuilder[F], SingleNodeBuilder[F]]] = {

    def singleNode(o: Options): SingleNodeBuilder[F] =
      mkSingleNodeBuilder[F](o, cfg).withLogger(logger)

    def cluster(o: Options): F[Option[ClusterBuilder[F]]] =
      mkClusterOptions[F](cfg).map(co => mkClusterBuilder[F](o, co, cfg).map(b => b.withLogger(logger)))

    mkOptions[F](cfg) >>= { o => cluster(o).map(_.toLeft(singleNode(o))) }

  }

  def mkSingleNodeBuilder[F[_]: Applicative](o: Options, cfg: Config): SingleNodeBuilder[F] = {

    val authority = cfg.option(s"$snPath.authority", _.getString)
    val address   = cfg.option(s"$snPath.address", _.getString).getOrElse(defaultAddress)
    val port      = cfg.option(s"$snPath.port", _.getInt).getOrElse(defaultPort)

    EsClient.singleNode[F](Endpoint(address, port), authority, o)
  }

  def mkClusterBuilder[F[_]: Applicative](
    o: Options,
    co: ClusterOptions,
    cfg: Config
  ): Option[ClusterBuilder[F]] = {

    val seed: Option[NonEmptySet[Endpoint]] = {

      val seeds: List[String] =
        cfg.option(s"$clPath.seed", _.getStringList).fold(List.empty[String])(_.asScala.toList)

      val result = seeds.flatMap { s =>
        s.split(":") match {
          case Array(host, port) => Endpoint(host, port.toIntOption.getOrElse(defaultPort)).some
          case Array(host)       => Endpoint(host, defaultPort).some
          case _                 => None
        }
      }

      result match {
        case x :: xs => NonEmptySet.of(x, xs: _*).some
        case Nil     => None
      }
    }

    val authority: Option[String] = cfg.option(s"$clPath.authority", _.getString)

    (seed, authority).mapN((s, a) => EsClient.cluster[F](s, a, o, co))

  }

  ///

  def mkOptions[F[_]: ApplicativeThrow](cfg: Config): F[Options] = {

    val certificate: Endo[Options] = o =>
      cfg
        .option(s"$root.certificate-path", _.getString)
        .map(new File(_))
        .fold(o.withInsecureMode)(o.withSecureMode)

    val credentials: Endo[Options] = o =>
      (cfg.option(s"$root.username", _.getString), cfg.option(s"$root.password", _.getString))
        .mapN(UserCredentials(_, _))
        .map(_.toOption)
        .fold(o)(o.withCredentials)

    val modifications: List[Endo[Options]] = List(
      o => cfg.option(s"$root.connection-name", _.getString).fold(o)(o.withConnectionName),
      certificate,
      credentials,
      o => cfg.durationOpt(s"$root.channel-shutdown-await").fold(o)(o.withChannelShutdownAwait),
      o => cfg.option(s"$ooPath.retry-enabled", _.getBoolean).fold(o)(o.withOperationsRetryEnabled),
      o => cfg.durationOpt(s"$ooPath.retry-delay").fold(o)(o.withOperationsRetryDelay),
      o => cfg.durationOpt(s"$ooPath.retry-max-delay").fold(o)(o.withOperationsRetryMaxDelay),
      o => cfg.option(s"$ooPath.retry-backoff-factor", _.getDouble).fold(o)(o.withOperationsRetryBackoffFactor),
      o => cfg.option(s"$ooPath.retry-max-attempts", _.getInt).fold(o)(o.withOperationsRetryMaxAttempts)
    )

    val mod = modifications.foldK

    Either.catchNonFatal(mod(Options.default)).liftTo[F]

  }

  ///

  def mkClusterOptions[F[_]: ApplicativeThrow](cfg: Config): F[ClusterOptions] = {

    val nodePreference: Endo[ClusterOptions] = o =>
      cfg.option(s"$coPath.node-preference", _.getString).map(_.toLowerCase()).fold(o) {
        case "leader"            => o.withNodePreference(NodePreference.Leader)
        case "follower"          => o.withNodePreference(NodePreference.Follower)
        case "read-only-replica" => o.withNodePreference(NodePreference.ReadOnlyReplica)
        case _                   => o
      }

    val maxDiscoverAttempts: Endo[ClusterOptions] = o =>
      cfg
        .option(s"$coPath.max-discovery-attempts", _.getInt)
        .fold(o)(a => if (a < 0) o.withMaxDiscoverAttempts(None) else o.withMaxDiscoverAttempts(a.some))

    val modifications: List[Endo[ClusterOptions]] = List(
      nodePreference,
      maxDiscoverAttempts,
      o => cfg.durationOpt(s"$coPath.retry-delay").fold(o)(o.withRetryDelay),
      o => cfg.durationOpt(s"$coPath.retry-max-delay").fold(o)(o.withRetryMaxDelay),
      o => cfg.option(s"$coPath.retry-backoff-factor", _.getDouble).fold(o)(o.withRetryBackoffFactor),
      o => cfg.durationOpt(s"$coPath.notification-interval").fold(o)(o.withNotificationInterval),
      o => cfg.durationOpt(s"$coPath.read-timeout").fold(o)(o.withReadTimeout)
    )

    val mod = modifications.foldK

    Either.catchNonFatal(mod(ClusterOptions.default)).liftTo[F]
  }

  implicit private class ConfigOps(val cfg: Config) extends AnyVal {

    def durationOpt(path: String): Option[FiniteDuration] = {
      def duration(path: String): FiniteDuration =
        FiniteDuration(cfg.getDuration(path, MILLISECONDS), MILLISECONDS).toCoarsest
      if (cfg hasPath path) Some(duration(path)) else None
    }

    def option[T](path: String, f: Config => (String => T)): Option[T] =
      if (cfg hasPath path) Option(f(cfg)(path)) else None
  }

}
