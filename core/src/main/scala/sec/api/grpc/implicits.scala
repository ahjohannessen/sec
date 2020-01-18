package sec
package api
package grpc

import io.grpc.Metadata
import grpc.constants.Headers.{Authorization, ConnectionName}

object implicits {

  private[grpc] object keys {
    val authKey: Metadata.Key[UserCredentials] = Metadata.Key.of(Authorization, UserCredentialsMarshaller)
    val cnKey: Metadata.Key[String]            = Metadata.Key.of(ConnectionName, StringMarshaller)
  }

  implicit final class ContextOps(val ctx: Context) extends AnyVal {
    def toMetadata: Metadata = {
      val md = new Metadata()
      ctx.userCreds.foreach(md.put(keys.authKey, _))
      md.put(keys.cnKey, ctx.connectionName)
      md
    }
  }

}
