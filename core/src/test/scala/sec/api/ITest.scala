package sec
package api

import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll
import org.scalacheck.Gen
import cats.data.{NonEmptyList => Nel}
import cats.effect.testing.specs2.CatsIO
import cats.effect._
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import sec.core._
import Arbitraries._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

trait ITest extends Specification with CatsIO with AfterAll {

  final private val testName = ITest.snakeCaseTransformation(getClass.getSimpleName)

  def genIdentifier: String                               = sampleOfGen(Gen.identifier.suchThat(id => id.length >= 5 && id.length <= 20))
  def genStreamId: StreamId.Id                            = genStreamId(s"${testName}_")
  def genStreamId(streamPrefix: String): StreamId.Id      = sampleOfGen(idGen.genStreamIdNormal(s"$streamPrefix"))
  def genEvents(n: Int): Nel[EventData]                   = genEvents(n, eventTypeGen.defaultPrefix)
  def genEvents(n: Int, etPrefix: String): Nel[EventData] = sampleOfGen(eventdataGen.eventDataNelN(n, etPrefix))

  final private lazy val (client, shutdown): (EsClient[IO], IO[Unit]) = {

    val builder = IO.delay(NettyChannelBuilder.forAddress("127.0.0.1", 2113).usePlaintext())

    val result: Resource[IO, EsClient[IO]] = for {
      b <- Resource.liftF(builder)
      l <- Resource.liftF(Slf4jLogger.fromName[IO]("integration-test"))
      c <- b.resourceWithShutdown[IO](mc => IO.delay(mc.shutdownNow()).void).map(EsClient[IO](_, Options.default, l))
    } yield c

    result.allocated.unsafeRunSync()
  }

  final lazy val esClient: EsClient[IO] = client
  final lazy val streams: Streams[IO]   = client.streams
  final lazy val gossip: Gossip[IO]     = client.gossip
  final lazy val meta: MetaStreams[IO]  = client.streams.metadata

  //

  final def afterAll(): Unit = shutdown.unsafeRunSync()

}

object ITest {
  import java.util.regex.Pattern

  final private val basePattern: Pattern = Pattern.compile("([A-Z]+)([A-Z][a-z])")
  final private val swapPattern: Pattern = Pattern.compile("([a-z\\d])([A-Z])")

  final private val snakeCaseTransformation: String => String = s => {
    val partial = basePattern.matcher(s).replaceAll("$1_$2")
    swapPattern.matcher(partial).replaceAll("$1_$2").toLowerCase
  }

}
