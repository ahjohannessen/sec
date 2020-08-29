package sec
package client

import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration
import cats.Endo
import cats.data._
import cats.implicits._
import cats.effect._
import io.grpc.{ManagedChannel, ManagedChannelBuilder, NameResolverRegistry}
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import io.chrisdavenport.log4cats.Logger
import sec.api._
import sec.api.cluster._
import sec.api.cluster.grpc.ResolverProvider

//======================================================================================================================

final private[sec] case class ChannelBuilderParams(
  targetOrEndpoint: Either[String, Endpoint],
  mode: ConnectionMode
)

private[sec] object ChannelBuilderParams {

  def apply(target: String, cm: ConnectionMode): ChannelBuilderParams =
    ChannelBuilderParams(target.asLeft, cm)

  def apply(endpoint: Endpoint, cm: ConnectionMode): ChannelBuilderParams =
    ChannelBuilderParams(endpoint.asRight, cm)

}

//======================================================================================================================

private[sec] trait OptionsBuilder[B <: OptionsBuilder[B]] {

  private[sec] def modOptions(fn: Options => Options): B

  def withCertificate(value: Path): B                       = modOptions(_.withSecureMode(value))
  def withConnectionName(value: String): B                  = modOptions(_.withConnectionName(value))
  def withCredentials(value: Option[UserCredentials]): B    = modOptions(_.withCredentials(value))
  def withOperationsRetryDelay(value: FiniteDuration): B    = modOptions(_.withOperationsRetryDelay(value))
  def withOperationsRetryMaxDelay(value: FiniteDuration): B = modOptions(_.withOperationsRetryMaxDelay(value))
  def withOperationsRetryMaxAttempts(value: Int): B         = modOptions(_.withOperationsRetryMaxAttempts(value))
  def withOperationsRetryBackoffFactor(value: Double): B    = modOptions(_.withOperationsRetryBackoffFactor(value))
  def withOperationsRetryEnabled: B                         = modOptions(_.withOperationsRetryEnabled)
  def withOperationsRetryDisabled: B                        = modOptions(_.withOperationsRetryDisabled)
}

//======================================================================================================================

sealed abstract class SingleNodeBuilder[F[_]](
  endpoint: Endpoint,
  authority: Option[String],
  options: Options,
  logger: Logger[F]
) extends OptionsBuilder[SingleNodeBuilder[F]] {

  private def copy(
    endpoint: Endpoint = endpoint,
    authority: Option[String] = authority,
    options: Options = options,
    logger: Logger[F] = logger
  ): SingleNodeBuilder[F] =
    new SingleNodeBuilder[F](endpoint, authority, options, logger) {}

  private[sec] def modOptions(fn: Options => Options): SingleNodeBuilder[F] =
    copy(options = fn(options))

  def withEndpoint(value: Endpoint): SingleNodeBuilder[F]        = copy(endpoint = value)
  def withAuthority(value: Option[String]): SingleNodeBuilder[F] = copy(authority = value)
  def withLogger(value: Logger[F]): SingleNodeBuilder[F]         = copy(logger = value)

  ///

  private[sec] def build[MCB <: ManagedChannelBuilder[MCB]](mcb: ChannelBuilderParams => F[MCB])(implicit
    F: ConcurrentEffect[F],
    T: Timer[F]
  ): Resource[F, EsClient[F]] = {

    val params: ChannelBuilderParams     = ChannelBuilderParams(endpoint, options.connectionMode)
    val channelBuilder: Resource[F, MCB] = Resource.liftF(mcb(params))
    val modifications: Endo[MCB]         = b => authority.fold(b)(a => b.overrideAuthority(a))

    channelBuilder >>= { builder =>
      modifications(builder).resource[F].map(EsClient[F](_, options, requiresLeader = false, logger))
    }
  }

}

object SingleNodeBuilder {

  private[sec] def apply[F[_]](
    endpoint: Endpoint,
    authority: Option[String],
    options: Options,
    logger: Logger[F]
  ): SingleNodeBuilder[F] =
    new SingleNodeBuilder[F](endpoint, authority, options, logger) {}
}

//======================================================================================================================

class ClusterBuilder[F[_]](
  seed: NonEmptySet[Endpoint],
  authority: String,
  options: Options,
  settings: ClusterSettings,
  logger: Logger[F]
) extends OptionsBuilder[ClusterBuilder[F]] {

  private def copy(
    seed: NonEmptySet[Endpoint] = seed,
    authority: String = authority,
    options: Options = options,
    settings: ClusterSettings = settings,
    logger: Logger[F] = logger
  ): ClusterBuilder[F] =
    new ClusterBuilder[F](seed, authority, options, settings, logger) {}

  private[sec] def modOptions(fn: Options => Options): ClusterBuilder[F] =
    copy(options = fn(options))

  def withClusterMaxDiscoveryAttempts(value: Int): ClusterBuilder[F]            = mod(_.withMaxDiscoverAttempts(value.some))
  def withClusterRetryDelay(value: FiniteDuration): ClusterBuilder[F]           = mod(_.withRetryDelay(value))
  def withClusterRetryMaxDelay(value: FiniteDuration): ClusterBuilder[F]        = mod(_.withRetryMaxDelay(value))
  def withClusterRetryBackoffFactor(value: Double): ClusterBuilder[F]           = mod(_.withRetryBackoffFactor(value))
  def withClusterReadTimeout(value: FiniteDuration): ClusterBuilder[F]          = mod(_.withReadTimeout(value))
  def withClusterNotificationInterval(value: FiniteDuration): ClusterBuilder[F] = mod(_.withNotificationInterval(value))
  def withClusterNodePreference(value: NodePreference): ClusterBuilder[F]       = mod(_.withNodePreference(value))
  def withAuthority(value: String): ClusterBuilder[F]                           = copy(authority = value)
  def withLogger(value: Logger[F]): ClusterBuilder[F]                           = copy(logger = value)

  ///

  private def mod(fn: ClusterSettings => ClusterSettings): ClusterBuilder[F] =
    copy(settings = fn(settings))

  private[sec] def build[MCB <: ManagedChannelBuilder[MCB]](
    mcb: ChannelBuilderParams => F[MCB]
  )(implicit
    F: ConcurrentEffect[F],
    T: Timer[F]
  ): Resource[F, EsClient[F]] = {

    val builderForTarget: String => F[MCB] =
      t => mcb(ChannelBuilderParams(t, options.connectionMode))

    def gossipFn(mc: ManagedChannel): Gossip[F] = Gossip(
      EsClient.mkGossipFs2Grpc[F](mc),
      EsClient.mkContext(options, requiresLeader = false),
      EsClient.mkOpts[F](options.operationOptions.copy(retryEnabled = false), logger, "Cluster.Gossip")
    )

    ClusterWatch(builderForTarget, settings, gossipFn, seed, authority, logger) >>= { cw =>

      val mkProvider: Resource[F, ResolverProvider[F]] = ResolverProvider
        .bestNodes[F](authority, settings.preference, cw.subscribe, logger)
        .evalTap(p => Sync[F].delay(NameResolverRegistry.getDefaultRegistry.register(p)))

      def builder(p: ResolverProvider[F]): Resource[F, MCB] =
        Resource.liftF(builderForTarget(s"${p.scheme}:///"))

      mkProvider >>= builder >>= {
        _.defaultLoadBalancingPolicy("round_robin")
          .overrideAuthority(authority)
          .resource[F]
          .map(EsClient[F](_, options, settings.preference.isLeader, logger))
      }

    }

  }

}

object ClusterBuilder {

  private[sec] def apply[F[_]: ConcurrentEffect: Timer](
    seed: NonEmptySet[Endpoint],
    authority: String,
    options: Options,
    settings: ClusterSettings,
    logger: Logger[F]
  ): ClusterBuilder[F] =
    new ClusterBuilder[F](seed, authority, options, settings, logger)
}

//======================================================================================================================
