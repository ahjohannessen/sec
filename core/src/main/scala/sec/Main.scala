package sec

import java.util.UUID
import scala.concurrent.duration._
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import fs2.Stream
import core._
import Streams._

object Main extends IOApp {

  val options = Options(UserCredentials.unsafe("admin", "changeit"))

  def nettyBuilder[F[_]: Sync]: F[NettyChannelBuilder] = Sync[F].delay {
    val ssl = GrpcSslContexts.forClient().trustManager(getClass.getResourceAsStream("/dev-cert.pem")).build()
    NettyChannelBuilder.forAddress("localhost", 2113).sslContext(ssl)
  }

  def run(args: List[String]): IO[ExitCode] = {

    val data = (1 to 20).toList.traverse { i =>
      Content.Json(f"""{ "a" : "data-$i%02d" }""") >>= (j => EventData("test-event", UUID.randomUUID(), j))
    }

    val stream = for {
      builder    <- Stream.eval(nettyBuilder[IO])
      client     <- EsClient.stream[IO, NettyChannelBuilder](builder, options).map(_.streams)
      streamName <- Stream.eval(uuid[IO].map(id => s"test_stream-$id"))
      eventData1 <- Stream.eval(data.map(l => NonEmptyList(l.head, l.tail)).orFail[IO])
      eventData2 <- Stream.eval(data.map(l => NonEmptyList(l.head, l.tail)).orFail[IO])
      _          <- Stream.eval(client.appendToStream(streamName, StreamRevision.NoStream, eventData1)).evalTap(print)
      _          <- Stream.eval(client.appendToStream(streamName, EventNumber.Exact(19).asRevision, eventData2)).evalTap(print)
      _ <- Stream.eval(
            client.setStreamMetadata(
              streamName,
              StreamRevision.NoStream,
              StreamMetaData.empty.copy(maxCount = 1.some),
              None
            ))
      _ <- Stream.eval(client.getStreamMetadata(streamName, None)).evalTap(r => IO(println(r)))
      _ <- client.readStreamForwards(streamName, EventNumber.Exact(39), 1).evalTap(print)
    } yield ()

    stream.compile.drain.as(ExitCode.Success)

  }

  def print(wr: WriteResult): IO[Unit] =
    IO.delay(println(wr))

  def print(r: Event): IO[Unit] =
    IO.delay(println(r.show))

  ///

  implicit class AttemptOps[A](a: Attempt[A]) {
    def orFail[F[_]: ErrorA]: F[A] = a.leftMap(new RuntimeException(_)).liftTo[F]
  }

}
