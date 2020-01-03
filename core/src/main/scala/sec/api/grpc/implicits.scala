package sec
package api
package grpc

import io.grpc.Metadata
import cats.implicits._

object implicits {

  implicit final class ContextOps(val ctx: Context) extends AnyVal {
    def toMetadata: Metadata = {
      val md = new Metadata()
      ctx.userCreds.foreach(md.put(keys.authKey, _))
      md.put(keys.cnKey, ctx.connectionName)
      md
    }
  }

}
