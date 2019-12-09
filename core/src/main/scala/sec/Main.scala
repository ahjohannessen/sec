package sec

import java.util.UUID
import scala.concurrent.duration._
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import io.grpc.netty.NettyChannelBuilder
import fs2.Stream
import core._
import StreamRevision.NoStream
import api._
import api.Streams._

object Main extends IOApp {

  def nettyBuilder[F[_]: Sync]: F[NettyChannelBuilder] = Sync[F].delay {
    NettyChannelBuilder.forAddress("localhost", 2113).usePlaintext()
  }

  def run(args: List[String]): IO[ExitCode] = {

    val result: Stream[IO, Unit] = for {
      builder <- Stream.eval(nettyBuilder[IO])
      client  <- EsClient.stream[IO, NettyChannelBuilder](builder, Options.default).map(_.streams)
      _       <- run2[IO](client)
    } yield ()

    result.compile.drain.as(ExitCode.Success)
  }

  def run1[F[_]: ConcurrentEffect](client: Streams[F]): Stream[F, Unit] = {

    val data = (1 to 20).toList.traverse { i =>
      Content.json(f"""{ "a" : "data-$i%02d" }""") >>= (j => EventData("test-event", UUID.randomUUID(), j))
    }

    for {
      streamId   <- Stream.eval(uuid[F].map(id => s"test_stream-$id"))
      eventData1 <- Stream.eval(data.map(l => NonEmptyList(l.head, l.tail)).orFail[F])
      eventData2 <- Stream.eval(data.map(l => NonEmptyList(l.head, l.tail)).orFail[F])
      _          <- Stream.eval(client.appendToStream(streamId, NoStream, eventData1)).evalTap(print[F])
      _          <- Stream.eval(client.appendToStream(streamId, EventNumber.Exact(19), eventData2)).evalTap(print[F])
      _          <- client.readStreamForwards(streamId, EventNumber.Start, 39).evalTap(print[F])
    } yield ()
  }

  def run2[F[_]: ConcurrentEffect: Timer](client: Streams[F]): Stream[F, Unit] = {

    val sid   = s"not-here-${UUID.randomUUID()}"
    val sub   = client.subscribeToStream(sid, Some(EventNumber.Exact(0))).evalMap(print[F])
    val event = (Content.json(f"""{ "a" : "b" }""") >>= (j => EventData("test", UUID.randomUUID(), j))).orFail[F]
    val app   = Stream.eval(event >>= (e => client.appendToStream(sid, NoStream, NonEmptyList.one(e)))).delayBy(5.second)

    // subscribeToStream works when stream does not exist.
    // I guess I have to start writing tests soon :)
    sub.concurrently(app)
  }

  ///

  def print[F[_]: Sync](wr: WriteResult): F[Unit] = Sync[F].delay(println(wr))
  def print[F[_]: Sync](e: Event): F[Unit]        = Sync[F].delay(println(e.show))

  implicit class AttemptOps[A](a: Attempt[A]) {
    def orFail[F[_]: ErrorA]: F[A] = a.leftMap(new RuntimeException(_)).liftTo[F]
  }

}
