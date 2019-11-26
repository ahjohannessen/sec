package sec
package api
package grpc

import java.nio.charset.StandardCharsets
import java.util.Base64
import io.grpc.Metadata.AsciiMarshaller
import constants.Headers.BasicScheme

//======================================================================================================================

private[grpc] object LongMarshaller extends AsciiMarshaller[Long] {
  def toAsciiString(value: Long): String         = value.toString
  def parseAsciiString(serialized: String): Long = serialized.toLong
}

//======================================================================================================================

private[grpc] object StringMarshaller extends AsciiMarshaller[String] {
  def toAsciiString(value: String): String         = value
  def parseAsciiString(serialized: String): String = serialized
}

//======================================================================================================================

private[grpc] object UserCredentialsMarshaller extends AsciiMarshaller[UserCredentials] {

  private val encoder64: Base64.Encoder = Base64.getEncoder

  def toAsciiString(uc: UserCredentials): String = {
    val encoded     = encoder64.encode(s"${uc.username}:${uc.password}".getBytes)
    val credentials = new String(encoded, StandardCharsets.US_ASCII)
    s"$BasicScheme $credentials"
  }

  def parseAsciiString(serialized: String): UserCredentials =
    UserCredentials.unsafe("<decoding>", "<not-supported>")

}

//======================================================================================================================
