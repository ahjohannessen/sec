package sec
package api
package grpc

import io.grpc.Metadata
import grpc.constants.Headers.{Authorization, ConnectionName, RequiresLeader}

object metadata {

  private[grpc] object keys {
    val authorization: Metadata.Key[UserCredentials] = Metadata.Key.of(Authorization, UserCredentialsMarshaller)
    val connectionName: Metadata.Key[String]         = Metadata.Key.of(ConnectionName, StringMarshaller)
    val requiresLeader: Metadata.Key[Boolean]        = Metadata.Key.of(RequiresLeader, BooleanMarshaller)
  }

  implicit final class ContextOps(val ctx: Context) extends AnyVal {
    def toMetadata: Metadata = {
      val md = new Metadata()
      ctx.userCreds.foreach(md.put(keys.authorization, _))
      md.put(keys.connectionName, ctx.connectionName)
      md.put(keys.requiresLeader, ctx.requiresLeader)
      md
    }
  }

}
