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
import cats._
import cats.data.{NonEmptyList, NonEmptySet}
import cats.syntax.all._
import cats.effect._
import com.comcast.ip4s.{Hostname, Port}
import io.grpc.ManagedChannelBuilder
import com.typesafe.config._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import sec.api._
import sec.api.cluster._

private[sec] object config {

  private val rootPath = "sec"
  private val ooPath   = s"$rootPath.operations"
  private val clPath   = s"$rootPath.cluster"
  private val coPath   = s"$clPath.options"

  //

  private val getAuthority: Config => Option[String] = cfg =>
    cfg.option(s"$rootPath.authority", _.getString).filter(_.trim.nonEmpty)

  //

  def mkClient[F[_]: Async, MCB <: ManagedChannelBuilder[MCB]](
    mcb: ChannelBuilderParams => F[MCB],
    cfg: Config
  ): Resource[F, EsClient[F]] = mkClient[F, MCB](mcb, cfg, NoOpLogger.impl[F], EndpointResolver.default[F])

  def mkClient[F[_]: Async, MCB <: ManagedChannelBuilder[MCB]](
    mcb: ChannelBuilderParams => F[MCB],
    cfg: Config,
    logger: Logger[F],
    er: EndpointResolver[F]
  ): Resource[F, EsClient[F]] =
    Resource.eval(mkBuilder[F](cfg, logger, er)) >>= {
      case Left(c)  => c.build(mcb)
      case Right(s) => s.build(mcb)
    }

  //

  def mkBuilder[F[_]: MonadThrow](
    cfg: Config,
    logger: Logger[F],
    er: EndpointResolver[F]
  ): F[Either[ClusterBuilder[F], SingleNodeBuilder[F]]] = {

    def singleNode(o: Options): SingleNodeBuilder[F] =
      mkSingleNodeBuilder[F](o, cfg).withLogger(logger)

    def cluster(o: Options): F[Option[ClusterBuilder[F]]] =
      mkClusterOptions[F](cfg).map(co =>
        mkClusterBuilder[F](o, co, cfg).map(b => b.withLogger(logger).withEndpointResolver(er)))

    mkOptions[F](cfg) >>= { o => cluster(o).map(_.toLeft(singleNode(o))) }

  }

  def mkSingleNodeBuilder[F[_]: Applicative](o: Options, cfg: Config): SingleNodeBuilder[F] = {

    val authority = getAuthority(cfg)
    val port      = cfg.portOpt(s"$rootPath.port").getOrElse(o.httpPort).value
    val address   = cfg.option(s"$rootPath.address", _.getString).getOrElse("127.0.0.1")

    EsClient.singleNode[F](Endpoint(address, port), authority, o)
  }

  def mkClusterBuilder[F[_]: ApplicativeThrow](
    o: Options,
    co: ClusterOptions,
    cfg: Config
  ): Option[ClusterBuilder[F]] = {

    type Authority = String

    val viaDns: Option[(ClusterEndpoints.ViaDns, Authority)] = {
      cfg
        .option(s"$clPath.dns", _.getString)
        .filter(_.nonEmpty)
        .flatMap(Hostname.fromString)
        .map(hn => (ClusterEndpoints.ViaDns(hn), getAuthority(cfg).getOrElse(hn.toString)))
    }

    val viaSeed: Option[(ClusterEndpoints.ViaSeed, Authority)] = {

      val defaultPort = o.httpPort.value
      val seedPath    = s"$clPath.seed"

      val seedsList: Option[NonEmptyList[String]] =
        cfg
          .option(seedPath, _.getStringList)
          .map(_.asScala.toList.map(_.trim).filter(_.nonEmpty))
          .flatMap(NonEmptyList.fromList)

      val seedsString: Option[NonEmptyList[String]] =
        cfg
          .option(seedPath, _.getString)
          .filter(_.nonEmpty)
          .map(_.split(",").toList.map(_.trim).filter(_.nonEmpty))
          .flatMap(NonEmptyList.fromList)

      val seeds = seedsList.orElse(seedsString).map(_.toList).getOrElse(Nil)

      val result = seeds.flatMap {
        _.split(":") match {
          case Array(host, port) => Endpoint(host, port.toIntOption.getOrElse(defaultPort)).some
          case Array(host)       => Endpoint(host, defaultPort).some
          case _                 => None
        }
      }

      result match {
        case x :: xs => getAuthority(cfg).map(a => (ClusterEndpoints.ViaSeed(NonEmptySet.of(x, xs: _*)), a))
        case Nil     => None
      }
    }

    val endpointsAndAuthority: Option[(ClusterEndpoints, Authority)] = viaDns.orElse(viaSeed)

    endpointsAndAuthority.map { case (ce, a) => EsClient.cluster[F](ce, a, o, co) }

  }

  //

  def mkOptions[F[_]: ApplicativeThrow](cfg: Config): F[Options] = {

    val certificate: Endo[Options] = o => {

      val certFile: Option[File] = cfg
        .option(s"$rootPath.certificate-path", _.getString)
        .filter(_.trim.nonEmpty)
        .map(new File(_))

      val certBase64: Option[String] = cfg
        .option(s"$rootPath.certificate-b64", _.getString)
        .filter(_.trim.nonEmpty)

      val cert: Option[Either[File, String]] =
        certFile.map(_.asLeft).orElse(certBase64.map(_.asRight))

      cert.fold(o.withInsecureMode)(_.fold(o.withSecureMode, o.withSecureMode))

    }

    val credentials: Endo[Options] = o =>
      (cfg.option(s"$rootPath.username", _.getString), cfg.option(s"$rootPath.password", _.getString))
        .mapN(UserCredentials(_, _))
        .map(_.toOption)
        .fold(o)(o.withCredentials)

    val modifications: List[Endo[Options]] = List(
      o => cfg.option(s"$rootPath.connection-name", _.getString).fold(o)(o.withConnectionName),
      certificate,
      credentials,
      o => cfg.durationOpt(s"$rootPath.channel-shutdown-await").fold(o)(o.withChannelShutdownAwait),
      o => cfg.option(s"$rootPath.prefetch-n-messages", _.getInt).fold(o)(o.withPrefetchN),
      o => cfg.portOpt(s"$rootPath.port").fold(o)(o.withHttpPort),
      o => cfg.option(s"$ooPath.retry-enabled", _.getBoolean).fold(o)(o.withOperationsRetryEnabled),
      o => cfg.durationOpt(s"$ooPath.retry-delay").fold(o)(o.withOperationsRetryDelay),
      o => cfg.durationOpt(s"$ooPath.retry-max-delay").fold(o)(o.withOperationsRetryMaxDelay),
      o => cfg.option(s"$ooPath.retry-backoff-factor", _.getDouble).fold(o)(o.withOperationsRetryBackoffFactor),
      o => cfg.option(s"$ooPath.retry-max-attempts", _.getInt).fold(o)(o.withOperationsRetryMaxAttempts)
    )

    val mod = modifications.foldK

    Either.catchNonFatal(mod(Options.default)).liftTo[F]

  }

  //

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

    def portOpt(path: String): Option[Port] =
      option(path, _.getInt).flatMap(Port.fromInt)

    def durationOpt(path: String): Option[FiniteDuration] = {
      def duration(path: String): FiniteDuration =
        FiniteDuration(cfg.getDuration(path, MILLISECONDS), MILLISECONDS).toCoarsest
      if (cfg hasPath path) Either.catchNonFatal(duration(path)).toOption else None
    }

    def option[T](path: String, f: Config => (String => T)): Option[T] =
      if (cfg hasPath path) Either.catchNonFatal(f(cfg)(path)).toOption else None
  }

}
