package sec
package api
package grpc

import io.grpc.Metadata
import org.specs2._
import cats.implicits._
import grpc.implicits._
import grpc.implicits.keys._
import grpc.constants.Headers._

class ImplicitsSpec extends mutable.Specification {

  "ContextOps.toMetadata" >> {

    val creds = UserCredentials.unsafe("hello", "world")
    val name  = "abc"

    val md1 = Context(None, name).toMetadata
    val md2 = Context(creds.some, name).toMetadata

    Option(md1.get(cnKey)) should beSome(name)
    Option(md1.get(authKey)) should beNone

    Option(md2.get(cnKey)) should beSome(name)
    Option(md2.get(Metadata.Key.of(Authorization, StringMarshaller))) should beSome("Basic aGVsbG86d29ybGQ=")

  }

}
