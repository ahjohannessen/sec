package sec

import java.util.logging.Logger
import scala.concurrent.duration._
import cats.data.NonEmptySet
import cats.implicits._
import cats.effect._
import io.grpc._
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall
import io.grpc.netty.NettyChannelBuilder
import com.eventstore.client.gossip.GossipFs2Grpc
import com.eventstore.client.streams.StreamsFs2Grpc
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import sec.api.{Context, Gossip, Streams}
import sec.api.Gossip.{ClusterInfo, Endpoint}
import sec.api.grpc.implicits._
import sec.api.grpc.convert.convertToEs
import sec.cluster.{ClusterWatch, Settings}
import sec.cluster.grpc.ResolverProvider
import sec.core.ServerUnavailable

object Demo extends IOApp {

  final val demoLogger = Logger.getLogger("Demo")

  def run(args: List[String]): IO[ExitCode] = IO.delay(println("Starting up")) *> run0

  ///

  val settings: Settings = Settings.default
  val options: Options   = Options.default

  val seed: NonEmptySet[Endpoint] = NonEmptySet.of(
    Endpoint(sys.env.getOrElse("ES1", "127.0.0.1"), 2113),
    Endpoint(sys.env.getOrElse("ES2", "127.0.0.1"), 2213),
    Endpoint(sys.env.getOrElse("ES3", "127.0.0.1"), 2313)
  )

  def authority: String = seed.head.address

  val gossipFn: ManagedChannel => Gossip[IO] =
    mc =>
      Gossip(
        GossipFs2Grpc.client[IO, Context](mc, (ct: Context) => ct.toMetadata, identity, convertToEs),
        options.defaultCreds,
        options.connectionName
      )

  val streamsFn: ManagedChannel => Streams[IO] =
    mc => Streams(StreamsFs2Grpc.client[IO, Context](mc, _.toMetadata, identity, convertToEs), options)

  def mkStreams(cw: ClusterWatch[IO]): Resource[IO, Gossip[IO]] = {

    def mkChannel[MCB <: ManagedChannelBuilder[MCB]](
      builderFromTarget: String => MCB,
      updates: fs2.Stream[IO, ClusterInfo]
    ): Resource[IO, ManagedChannel] =
      builderFromTarget(ResolverProvider.clusterScheme)
        .nameResolverFactory(ResolverProvider.bestNodes(authority, settings.preference, updates))
        .usePlaintext()
        .defaultLoadBalancingPolicy("round_robin")
        .resource[IO]

    mkChannel(NettyChannelBuilder.forTarget, cw.subscribe).map(gossipFn)
  }

  def run0: IO[ExitCode] = {

    val result: Resource[IO, Gossip[IO]] = for {
      watch <- ClusterWatch(
                 t => NettyChannelBuilder.forTarget(t).usePlaintext(),
                 settings,
                 gossipFn,
                 seed,
                 authority
               )
      streams <- mkStreams(watch)
    } yield streams

    result.use { x =>

      def run: fs2.Stream[IO, ClusterInfo] = fs2.Stream
        .eval(x.read(None))
        .handleErrorWith {
          case _: ServerUnavailable => run
          case th                   => fs2.Stream.raiseError[IO](th)
        }

      run
        .map(x => demoLogger.info(x.show))
        .repeat
        .metered(1.second)
        .take(5)
        .compile
        .drain
        .as(ExitCode.Success)
    }

  }

  object HeaderClientInterceptor extends ClientInterceptor {

    final val logger = Logger.getLogger("HeaderClientInterceptor")

    def interceptCall[ReqT, RespT](
      method: MethodDescriptor[ReqT, RespT],
      callOptions: CallOptions,
      next: Channel
    ): ClientCall[ReqT, RespT] = new SimpleForwardingClientCall[ReqT, RespT](next.newCall(method, callOptions)) {

      override def start(responseListener: ClientCall.Listener[RespT], headers: Metadata): Unit = {
        logger.info(s"request headers: ${headers.toString}")
        super.start(
          new ForwardingClientCallListener.SimpleForwardingClientCallListener[RespT](responseListener) {
            override def onHeaders(headers: Metadata): Unit = {
              logger.info(s"response headers: ${headers.toString}")
              super.onHeaders(headers)
            }

            override def onClose(status: Status, trailers: Metadata): Unit = {
              logger.info(s"response status: ${status.toString} ${trailers.toString}")
              super.onClose(status, trailers)
            }
          },
          headers
        )
      }
    }
  }

}
