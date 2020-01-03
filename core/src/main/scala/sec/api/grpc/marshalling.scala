package sec
package api
package grpc

import java.nio.charset.StandardCharsets
import java.util.Base64
import io.grpc.Metadata.AsciiMarshaller
import constants.Headers.BasicScheme

//======================================================================================================================

private[grpc] object IntMarshaller  extends NumericAsciiMarshaller[Int]("Int")
private[grpc] object LongMarshaller extends NumericAsciiMarshaller[Long]("Long")

private[grpc] abstract sealed class NumericAsciiMarshaller[T: Numeric](tpe: String) extends AsciiMarshaller[T] {
  final def toAsciiString(v: T): String    = v.toString
  final def parseAsciiString(s: String): T = Numeric[T].parseString(s).getOrElse(throw InvalidInput(s, tpe))
}

private[grpc] final case class InvalidInput(input: String, tpe: String)
  extends RuntimeException(s"Could not parse $input to $tpe")

//======================================================================================================================

private[grpc] object StringMarshaller extends AsciiMarshaller[String] {
  def toAsciiString(value: String): String         = value
  def parseAsciiString(serialized: String): String = serialized
}

//======================================================================================================================

private[grpc] object UserCredentialsMarshaller extends AsciiMarshaller[UserCredentials] {

  val decodingNotSupported      = UserCredentials.unsafe("decoding-not-supported", "n/a")
  val encoder64: Base64.Encoder = Base64.getEncoder

  def toAsciiString(uc: UserCredentials): String = {
    val encoded     = encoder64.encode(s"${uc.username}:${uc.password}".getBytes)
    val credentials = new String(encoded, StandardCharsets.US_ASCII)
    s"$BasicScheme $credentials"
  }

  def parseAsciiString(serialized: String): UserCredentials = decodingNotSupported

}

//======================================================================================================================
