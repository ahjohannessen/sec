package sec

import io.grpc.Metadata
import sec.core.UserCredentials
import grpc.Constants.Headers.Authorization

package object grpc {

  private object keys {
    val authKey: Metadata.Key[UserCredentials] = Metadata.Key.of(Authorization, UserCredentialsMarshaller)
  }

  implicit final class UserCredentialsOps(val uc: UserCredentials) extends AnyVal {
    def toMetadata: Metadata = {
      val md = new Metadata()
      md.put(keys.authKey, uc)
      md
    }
  }

}
