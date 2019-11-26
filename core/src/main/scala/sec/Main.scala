package sec

import java.util.UUID
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import io.grpc.netty.NettyChannelBuilder
import fs2.Stream
import core._
import api._
import api.Streams._

object Main extends IOApp {

  def nettyBuilder[F[_]: Sync]: F[NettyChannelBuilder] = Sync[F].delay {
    NettyChannelBuilder.forAddress("localhost", 2113).usePlaintext()
  }

  def run(args: List[String]): IO[ExitCode] = {

    val data = (1 to 20).toList.traverse { i =>
      Content.json(f"""{ "a" : "data-$i%02d" }""") >>= (j => EventData("test-event", UUID.randomUUID(), j))
    }

    val stream = for {
      builder    <- Stream.eval(nettyBuilder[IO])
      client     <- EsClient.stream[IO, NettyChannelBuilder](builder, Options.default).map(_.streams)
      streamName <- Stream.eval(uuid[IO].map(id => s"test_stream-$id"))
      eventData1 <- Stream.eval(data.map(l => NonEmptyList(l.head, l.tail)).orFail[IO])
      eventData2 <- Stream.eval(data.map(l => NonEmptyList(l.head, l.tail)).orFail[IO])
      _          <- Stream.eval(client.appendToStream(streamName, StreamRevision.NoStream, eventData1)).evalTap(print)
      _          <- Stream.eval(client.appendToStream(streamName, EventNumber.Exact(19), eventData2)).evalTap(print)
      _ <- Stream.eval(
            client.metadata.setStreamMetadata(
              streamName,
              StreamRevision.NoStream,
              StreamState.empty.copy(maxCount = 1000.some),
              None
            ))
      _ <- Stream.eval(client.metadata.getStreamMetadata(streamName, None)).evalTap(r => IO(println(r)))
      _ <- client.readStreamForwards(streamName, EventNumber.Start, 39).evalTap(print)
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
