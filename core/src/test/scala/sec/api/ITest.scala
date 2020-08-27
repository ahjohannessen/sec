package sec
package api

import java.io.File
import org.specs2.mutable.Specification
import org.specs2.specification.AfterAll
import org.scalacheck.Gen
import cats.data.{NonEmptyList => Nel}
import cats.effect.testing.specs2.CatsIO
import cats.effect._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.lyranthe.fs2_grpc.java_runtime.implicits._
import io.grpc.netty.{GrpcSslContexts, NettyChannelBuilder}
import sec.core._
import sec.client._
import Arbitraries._

trait ITest extends Specification with CatsIO with AfterAll {

  final private val testName = ITest.snakeCaseTransformation(getClass.getSimpleName)

  def genIdentifier: String                               = sampleOfGen(Gen.identifier.suchThat(id => id.length >= 5 && id.length <= 20))
  def genStreamId: StreamId.Id                            = genStreamId(s"${testName}_")
  def genStreamId(streamPrefix: String): StreamId.Id      = sampleOfGen(idGen.genStreamIdNormal(s"$streamPrefix"))
  def genEvents(n: Int): Nel[EventData]                   = genEvents(n, eventTypeGen.defaultPrefix)
  def genEvents(n: Int, etPrefix: String): Nel[EventData] = sampleOfGen(eventdataGen.eventDataNelN(n, etPrefix))

  final private lazy val (client, shutdown): (EsClient[IO], IO[Unit]) = {

    val certsFolder = new File(sys.env.getOrElse("SEC_TEST_CERTS_PATH", TestBuildInfo.certsPath))
    val ca          = new File(certsFolder, "ca/ca.crt")

    val address   = sys.env.getOrElse("SEC_IT_TEST_HOST_ADDRESS", "127.0.0.1")
    val port      = sys.env.get("SEC_IT_TEST_HOST_PORT").flatMap(_.toIntOption).getOrElse(2113)
    val authority = sys.env.getOrElse("SEC_IT_TEST_AUTHORITY", "es.sec.local")

    val builder = IO.delay(
      NettyChannelBuilder
        .forAddress(address, port)
        .useTransportSecurity()
        .sslContext(GrpcSslContexts.forClient().trustManager(ca).build())
        .overrideAuthority(authority)
    )

    val options = Options.default.withOperationsRetryMaxAttempts(3)

    val result: Resource[IO, EsClient[IO]] = for {
      b <- Resource.liftF(builder)
      l <- Resource.liftF(Slf4jLogger.fromName[IO]("integration-test"))
      c <- b.resourceWithShutdown[IO](mc => IO.delay(mc.shutdownNow()).void).map(EsClient[IO](_, options, true, l))
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
