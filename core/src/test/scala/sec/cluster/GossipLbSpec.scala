package sec
package cluster

import scala.concurrent.duration._
import cats.data.{NonEmptyList => Nel}
import cats.implicits._
import cats.effect._
import cats.effect.testing.specs2.CatsIO
import fs2.Stream
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import io.grpc.netty.{NettyChannelBuilder, NettyServerBuilder}
import io.grpc._
import org.specs2.mutable.Specification

class GossipLbSpec extends Specification with CatsIO {

  import GossipLbSpec._

  "Yep" >> {

    "looking into java land" >> {

      val resources = for {
        s1 <- server.server[IO]("s1", 9997, client.seed)
        s2 <- server.server[IO]("s2", 9998, client.seed)
        s3 <- server.server[IO]("s3", 9999, client.seed)
        w  <- client.watch[IO]
      } yield (s1, s2, s3, w)

      resources.use {
        case (s1, s2, s3, w) =>
          val run = for {
            _ <- Stream(
                   Stream.eval(IO.delay(s1.start()) *> IO.sleep(2.second) *> IO.delay(s1.shutdown())),
                   Stream.eval(IO.delay(s2.start()) *> IO.sleep(4.second) *> IO.delay(s2.shutdown())),
                   Stream.eval(IO.delay(s3.start())),
                   w.subscribe
                 ).parJoinUnbounded
          } yield ()

          Stream.sleep(6.second).mergeHaltBoth(run).compile.drain.as(ok)

      }
    }

  }

}

object GossipLbSpec {

  object client {

    import sec.api._
    import sec.api.grpc.implicits._
    import sec.api.grpc.convert.convertToEs
    import sec.api.Gossip._
    import com.eventstore.client.gossip.GossipFs2Grpc

    val authority: String  = "127.0.0.1"
    val settings: Settings = Settings.default.copy(notificationInterval = 500.millis)
    val options: Options   = Options("cluster-watch", NodePreference.Random, None)

    val seed: Nel[Endpoint] = Nel.of(
      Endpoint("127.0.0.1", 9997),
      Endpoint("127.0.0.1", 9998),
      Endpoint("127.0.0.1", 9999)
    )

    def gossipFn[F[_]: ConcurrentEffect: Timer](ch: ManagedChannel): Gossip[F] =
      Gossip[F](GossipFs2Grpc.client[F, Context](ch, _.toMetadata, errorAdapter = convertToEs), options)

    val builder: String => NettyChannelBuilder = t => NettyChannelBuilder.forTarget(t).usePlaintext()

    def watch[F[_]: ConcurrentEffect: Timer]: Resource[F, ClusterWatch[F]] =
      ClusterWatch(builder, settings, gossipFn[F], seed, authority)

  }

  object server {

    import sec.api.Gossip.Endpoint
    import sec.api.mapping.shared._
    import com.eventstore.client._
    import com.eventstore.client.gossip._

    def server[F[_]: ConcurrentEffect](name: String, port: Int, seed: Nel[Endpoint]): Resource[F, Server] =
      NettyServerBuilder.forPort(port).addService(service(name, seed)).resource[F]

    def service[F[_]: ConcurrentEffect](serverName: String, seed: Nel[Endpoint]): ServerServiceDefinition =
      GossipFs2Grpc.service[F, Metadata](new GossipService[F](serverName, seed), _.asRight)

    class GossipService[F[_]](serverName: String, seed: Nel[Endpoint])(implicit F: Sync[F])
      extends GossipFs2Grpc[F, Metadata] {

      println(s"Starting $serverName")

      val endpointId: Endpoint => java.util.UUID = {
        case Endpoint("127.0.0.1", 9997) => java.util.UUID.fromString("67e0c45c-b618-42ab-bdfc-0b0706a8daa7")
        case Endpoint("127.0.0.1", 9998) => java.util.UUID.fromString("d0608121-852d-4bf0-8cde-09f68d644cd3")
        case Endpoint("127.0.0.1", 9999) => java.util.UUID.fromString("9eb33ad0-2bba-4a7b-99b7-abe2bfc82afa")
        case _: Endpoint                 => java.util.UUID.randomUUID()
      }

      def mkMember(vstate: MemberInfo.VNodeState, isAlive: Boolean, endpoint: Endpoint): MemberInfo =
        MemberInfo()
          .withInstanceId(mkUuid(endpointId(endpoint)))
          .withTimeStamp(0L)
          .withState(vstate)
          .withIsAlive(isAlive)
          .withHttpEndPoint(
            EndPoint(endpoint.address, endpoint.port)
          )

      def read(request: Empty, ctx: Metadata): F[ClusterInfo] = {
        val head    = mkMember(MemberInfo.VNodeState.Leader, true, seed.head)
        val rest    = seed.tail.map(mkMember(MemberInfo.VNodeState.Follower, true, _))
        val members = scala.util.Random.shuffle(head +: rest)

        F.delay(ClusterInfo(members.take(scala.util.Random.between(2, members.size))))

      }
    }

  }

}
