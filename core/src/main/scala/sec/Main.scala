package sec

import java.util.UUID
import cats.data.NonEmptyList
import cats.ApplicativeError
import cats.implicits._
import cats.effect._
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import io.grpc.Metadata
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import com.eventstore.client.streams._
import fs2.Stream
import core._
import format._
import EsClient.Config

object Main extends IOApp {

  val cfg = Config(UserCredentials.unsafe("admin", "changeit"))
  val ssl = GrpcSslContexts.forClient().trustManager(getClass.getResourceAsStream("/dev-cert.pem")).build()

  def run(args: List[String]): IO[ExitCode] = {

    val mkChannel = NettyChannelBuilder.forAddress("localhost", 2113).sslContext(ssl).stream[IO]

    val data = (1 to 20).toList.traverse { i =>
      Content.Json(f"""{ "a" : "data-$i%02d" }""") >>= (j => EventData("test-event", UUID.randomUUID(), j))
    }

    val stream = for {
      client     <- mkChannel.map(ch => EsClient(Streams.client[IO, Metadata](ch, identity), cfg))
      eventData1 <- Stream.eval(data.map(l => NonEmptyList(l.head, l.tail)).orFail[IO])
      eventData2 <- Stream.eval(data.map(l => NonEmptyList(l.head, l.tail)).orFail[IO])
      streamName <- Stream.eval(uuid[IO].map(id => s"test_stream-$id"))
      _          <- Stream.eval(client.appendToStream(streamName, StreamRevision.NoStream, eventData1)).evalTap(print)
      en         <- Stream.eval(EventNumber.Exact(19).toRight(new RuntimeException("OhNoes")).liftTo[IO])
      _          <- Stream.eval(client.appendToStream(streamName, en.asRevision, eventData2)).evalTap(print)
      _          <- client.readStreamForwards(streamName, EventNumber.Start, 20)
    } yield ()

    stream.compile.drain.as(ExitCode.Success)

  }

  def print(wr: WriteResult): IO[Unit] =
    IO.delay(println(s"$wr"))

  def print(r: ReadResp): IO[Unit] = IO.delay {
    val response = r.event.flatMap(_.event.filter(_.streamName.startsWith("test_stream")).map { e =>
      s"${e.streamName} @ ${e.streamRevision} -> ${e.data.toBV.decodeUtf8} : ${e.id.toUUID} : ${e.metadata}"
    })

    response.fold(())(println)
  }

  ///

  implicit class AttemptOps[A](a: Attempt[A]) {
    def orFail[F[_]: ApplicativeError[*[_], Throwable]]: F[A] =
      a.leftMap(new RuntimeException(_)).liftTo[F]
  }

}
