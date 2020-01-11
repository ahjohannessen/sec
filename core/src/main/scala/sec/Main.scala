package sec

import java.util.UUID
import scala.concurrent.duration._
import cats.data.NonEmptyList
import cats.implicits._
import cats.effect._
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import fs2.Stream
import core._
import StreamRevision.NoStream
import api._
import api.Streams._

object Main extends IOApp {

  def nettyBuilder[F[_]: Sync]: F[NettyChannelBuilder] = Sync[F].delay {
    val ssl = GrpcSslContexts.forClient().trustManager(getClass.getResourceAsStream("/dev-cert.pem")).build()
    NettyChannelBuilder.forAddress("localhost", 2113).sslContext(ssl)
  }

  def run(args: List[String]): IO[ExitCode] = {

    val result: Stream[IO, Unit] = for {
      builder <- Stream.eval(nettyBuilder[IO])
      client  <- EsClient.stream[IO, NettyChannelBuilder](builder, Options.default).map(_.streams)
      _       <- run6[IO](client)
    } yield ()

    result.compile.drain.as(ExitCode.Success)
  }

  def run1[F[_]: ConcurrentEffect](client: Streams[F]): Stream[F, Unit] = {

    val data = (1 to 20).toList.traverse { i =>
      Content.json(f"""{ "a" : "data-$i%02d" }""") >>= (j => EventData("test-event", UUID.randomUUID(), j))
    }

    for {
      streamId   <- Stream.eval(uuid[F] >>= (id => StreamId[F](s"test_stream-$id")))
      eventData1 <- Stream.eval(data.map(l => NonEmptyList(l.head, l.tail)).orFail[F])
      eventData2 <- Stream.eval(data.map(l => NonEmptyList(l.head, l.tail)).orFail[F])
      _          <- Stream.eval(client.appendToStream(streamId, NoStream, eventData1)).evalTap(print[F])
      _          <- Stream.eval(client.appendToStream(streamId, EventNumber.exact(19), eventData2)).evalTap(print[F])
      _          <- client.readStreamForwards(streamId, EventNumber.Start, 39).evalTap(print[F])
    } yield ()
  }

  def run2[F[_]: ConcurrentEffect: Timer](client: Streams[F]): Stream[F, Unit] = {

    val streamId           = StreamId[F](s"not-here-${UUID.randomUUID()}")
    def sub(sid: StreamId) = client.subscribeToStream(sid, Some(EventNumber.Start)).evalMap(print[F])
    val event              = (Content.json(f"""{ "a" : "b" }""") >>= (j => EventData("test", UUID.randomUUID(), j))).orFail[F]
    def app(id: StreamId)  = Stream.eval(event >>= (e => client.appendToStream(id, NoStream, NonEmptyList.one(e))))

    // subscribeToStream works when stream does not exist.
    // I guess I have to start writing tests soon :)

    Stream.eval(streamId) >>= { sid =>
      sub(sid).concurrently(app(sid).delayBy(5.second))
    }

  }

  def run3[F[_]: ConcurrentEffect](client: Streams[F]): Stream[F, Unit] =
    client.readAllBackwards(Position.End, 1).take(1).evalTap(print[F]).void

  def run4[F[_]: ConcurrentEffect](client: Streams[F]): Stream[F, Unit] =
    Stream.eval(StreamId[F]("$users")) >>= { sid =>
      client
        .readStreamBackwards(sid, EventNumber.End, 1)
        .take(1)
        .evalTap(print[F])
        .void
    }

  def run5[F[_]](client: Streams[F])(implicit F: ConcurrentEffect[F]): Stream[F, Unit] = {

    val streamId              = StreamId[F](s"delete-${UUID.randomUUID()}")
    val event                 = (Content.json(f"""{ "de" : "l" }""") >>= (j => EventData("test", UUID.randomUUID(), j))).orFail[F]
    def append(sid: StreamId) = Stream.eval(event >>= (e => client.appendToStream(sid, NoStream, NonEmptyList.one(e))))
    def read(sid: StreamId)   = client.readStreamForwards(sid, EventNumber.Start, 1).take(1).evalTap(print[F]).void
    def delete(sid: StreamId) = Stream.eval(client.softDelete(sid, EventNumber.Start))

    for {
      sid <- Stream.eval(streamId)
      _   <- append(sid)
      _   <- read(sid)
      _   <- delete(sid)
      _ <- read(sid).recoverWith {
            case snf: StreamNotFound =>
              Stream
                .eval(client.metadata.getStreamMetadata(sid, None))
                .collect { case Some(v) => v.data.truncateBefore.fold(0L)(_.value) }
                .evalMap { n =>
                  if (n == Long.MaxValue) F.raiseError(StreamDeleted(sid.stringValue)) else F.raiseError(snf)
                }
                .void
          }
      _ <- append(sid)
      _ <- read(sid)
    } yield ()

  }

  def run6[F[_]: ConcurrentEffect](client: Streams[F]): Stream[F, Unit] =
    client.readAllForwards(Position.Start, Int.MaxValue).evalTap(print[F]).void

  ///

  def print[F[_]: Sync](v: AnyRef): F[Unit] = Sync[F].delay(println(v))
  def print[F[_]: Sync](e: Event): F[Unit]  = Sync[F].delay(println(e.show))

  implicit class AttemptOps[A](a: Attempt[A]) {
    def orFail[F[_]: ErrorA]: F[A] = a.leftMap(new RuntimeException(_)).liftTo[F]
  }

}
