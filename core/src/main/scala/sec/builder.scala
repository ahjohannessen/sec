package sec

import java.io.InputStream
import java.nio.file.Path
import cats.implicits._
import api.Endpoint

/// Initial WIP

sealed private[sec] trait ConnectionMode
private[sec] object ConnectionMode {
  case object Insecure                                                  extends ConnectionMode
  final case class Secure(certChain: Option[Either[InputStream, Path]]) extends ConnectionMode
}

final private[sec] case class ChannelBuilderParams(
  targetOrEndpoint: Either[String, Endpoint],
  connectionMode: ConnectionMode
)

final private[sec] object ChannelBuilderParams {

  def apply(target: String, certChain: InputStream): ChannelBuilderParams =
    ChannelBuilderParams(target.asLeft, ConnectionMode.Secure(certChain.asLeft.some))

  def apply(target: String, certChain: Path): ChannelBuilderParams =
    ChannelBuilderParams(target.asLeft, ConnectionMode.Secure(certChain.asRight.some))

  def apply(endpoint: Endpoint, certChain: Path): ChannelBuilderParams =
    ChannelBuilderParams(endpoint.asRight, ConnectionMode.Secure(certChain.asRight.some))

}
