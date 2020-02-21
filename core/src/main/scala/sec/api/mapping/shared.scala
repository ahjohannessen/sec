package sec
package api
package mapping

import java.util.{UUID => JUUID}
import cats.implicits._
import com.eventstore.client.shared.UUID

object shared {

  val mkUuid: JUUID => UUID = j =>
    UUID().withStructured(UUID.Structured(j.getMostSignificantBits(), j.getLeastSignificantBits()))

  def mkJuuid[F[_]: ErrorA](uuid: UUID): F[JUUID] = {

    val juuid = uuid.value match {
      case UUID.Value.Structured(v) => new JUUID(v.mostSignificantBits, v.leastSignificantBits).asRight
      case UUID.Value.String(v)     => Either.catchNonFatal(JUUID.fromString(v)).leftMap(_.getMessage())
      case UUID.Value.Empty         => "UUID is missing".asLeft
    }

    juuid.leftMap(ProtoResultError).liftTo[F]
  }

}
