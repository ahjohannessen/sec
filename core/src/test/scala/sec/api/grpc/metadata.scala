package sec
package api
package grpc

import io.grpc.Metadata
import org.specs2._
import cats.implicits._
import grpc.metadata._
import grpc.metadata.keys._
import grpc.constants.Headers._

class MetadataSpec extends mutable.Specification {

  "ContextOps.toMetadata" >> {

    val creds = UserCredentials.unsafe("hello", "world")
    val name  = "abc"

    val md1 = Context(name, None, false).toMetadata
    val md2 = Context(name, creds.some, true).toMetadata

    Option(md1.get(connectionName)) should beSome(name)
    Option(md1.get(authorization)) should beNone
    Option(md1.get(requiresLeader)) should beSome(false)

    Option(md2.get(connectionName)) should beSome(name)
    Option(md2.get(Metadata.Key.of(Authorization, StringMarshaller))) should beSome("Basic aGVsbG86d29ybGQ=")
    Option(md2.get(requiresLeader)) should beSome(true)

  }

}
