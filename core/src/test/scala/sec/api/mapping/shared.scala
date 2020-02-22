package sec
package api
package mapping

import java.util.{UUID => JUUID}
import cats.implicits._
import org.specs2._
import com.eventstore.client.shared._
import sec.api.mapping.shared._

class SharedMappingSpec extends mutable.Specification {

  type ErrorOr[A] = Either[Throwable, A]

  "shared" >> {

    val uuidString     = "e5390fcb-48bd-4895-bcc3-01629cca2af6"
    val juuid          = JUUID.fromString(uuidString)
    val uuidStructured = UUID.Structured(juuid.getMostSignificantBits(), juuid.getLeastSignificantBits())

    "mkJuuid" >> {
      mkJuuid[ErrorOr](UUID().withValue(UUID.Value.Empty)) shouldEqual ProtoResultError("UUID is missing").asLeft
      mkJuuid[ErrorOr](UUID().withString(uuidString)) shouldEqual juuid.asRight
      mkJuuid[ErrorOr](UUID().withStructured(uuidStructured)) shouldEqual juuid.asRight
    }

    "mkUuid" >> {
      mkUuid(juuid) shouldEqual UUID().withStructured(uuidStructured)
    }

  }

}
