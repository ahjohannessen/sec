package sec

import scala.concurrent.duration._
import cats.data.NonEmptySet
import cats.implicits._
import cats.effect._
import io.grpc._
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.chrisdavenport.log4cats.Logger
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import sec.EsClient.{mkContext, mkGossipClient, mkStreamsClient, mkStreamsOpts}
import sec.api.{Endpoint, Gossip, Streams}
import sec.api.Gossip.ClusterInfo
import sec.api.cluster.{ClusterWatch, Settings}
import sec.api.cluster.grpc.ResolverProvider
import sec.core._
import sec.client.Netty

object Demo extends IOApp {

  def run(args: List[String]): IO[ExitCode] = log.info("Starting up") *> run0

  ///

  val log: Logger[IO]    = Slf4jLogger.fromName[IO]("Demo").unsafeRunSync()
  val settings: Settings = Settings.default.copy(maxDiscoverAttempts = 25.some)
  val options: Options   = Options.default
  val authority: String  = "es.demo.local"

  val seed: NonEmptySet[Endpoint] = NonEmptySet.of(
    getEndpoint("ES1_ADDRESS", "ES1_PORT", "192.168.104.101", 2113),
    getEndpoint("ES2_ADDRESS", "ES2_PORT", "192.168.104.102", 2113),
    getEndpoint("ES3_ADDRESS", "ES3_PORT", "192.168.104.103", 2113)
  )

  ///

  def getEndpoint(envAddr: String, envPort: String, fallbackAddr: String, fallbackPort: Int): Endpoint = {
    val address = sys.env.getOrElse(envAddr, fallbackAddr)
    val port    = sys.env.get(envPort).flatMap(_.toIntOption).getOrElse(fallbackPort)
    Endpoint(address, port)
  }

  def builderForTarget(t: String): IO[NettyChannelBuilder] =
    Netty.mkBuilder[IO](ChannelBuilderParams(t, getClass.getResourceAsStream("/certs/ca.crt")))

  def gossipFn(mc: ManagedChannel, requiresLeader: Boolean): Gossip[IO] =
    Gossip(mkGossipClient[IO](mc), mkContext(options, requiresLeader))

  def streamsFn(mc: ManagedChannel, requiresLeader: Boolean): Streams[IO] =
    Streams(mkStreamsClient[IO](mc), mkContext(options, requiresLeader), mkStreamsOpts(options.operationOptions, log))

  def mkStreams(cw: ClusterWatch[IO]): Resource[IO, Gossip[IO]] = {

    val mkProvider: Resource[IO, ResolverProvider[IO]] = ResolverProvider
      .bestNodes(authority, settings.preference, cw.subscribe, log)
      .evalTap(p => IO.delay(NameResolverRegistry.getDefaultRegistry.register(p)))

    val mkChannel: Resource[IO, ManagedChannel] =
      mkProvider >>= { p =>
        Resource.liftF(builderForTarget(s"${p.scheme}:///")) >>= {
          _.defaultLoadBalancingPolicy("round_robin").resource[IO]
        }
      }

    mkChannel.map(gossipFn(_, options.nodePreference.isLeader))
  }

  def run0: IO[ExitCode] = {

    val result: Resource[IO, Gossip[IO]] =
      ClusterWatch(builderForTarget, settings, gossipFn(_, false), seed, authority, log) >>= mkStreams

    result.use { x =>

      def run: fs2.Stream[IO, ClusterInfo] = fs2.Stream
        .eval(x.read(None))
        .handleErrorWith {
          case _: ServerUnavailable => run
          case th                   => fs2.Stream.raiseError[IO](th)
        }

      run
        .evalMap(x => log.info(x.show))
        .repeat
        .metered(500.millis)
        .take(10)
        .compile
        .drain
        .as(ExitCode.Success)
    }

  }

}
