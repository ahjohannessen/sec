package sec
package api
package mapping

import java.util.{UUID => JUUID}
import cats.implicits._
import com.google.protobuf.ByteString
import com.eventstore.client.{StreamIdentifier, UUID}
import sec.core.StreamId

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

  //

  def mkStreamId[F[_]: ErrorM](sid: Option[StreamIdentifier]): F[StreamId] =
    mkStreamId[F](sid.getOrElse(StreamIdentifier()))

  def mkStreamId[F[_]: ErrorM](sid: StreamIdentifier): F[StreamId] =
    sid.utf8[F] >>= { sidStr =>
      StreamId.stringToStreamId(sidStr).leftMap(ProtoResultError).liftTo[F]
    }

  final implicit class StreamIdOps(val v: StreamId) extends AnyVal {
    def esSid: StreamIdentifier = v.stringValue.toStreamIdentifer
  }

  final implicit class StringOps(val v: String) extends AnyVal {
    def toStreamIdentifer: StreamIdentifier = StreamIdentifier(ByteString.copyFromUtf8(v))
  }

  final implicit class StreamIdentifierOps(val v: StreamIdentifier) extends AnyVal {
    def utf8[F[_]](implicit F: ErrorA[F]): F[String] =
      F.catchNonFatal(Option(v.streamName.toStringUtf8()).getOrElse(""))
  }

  //

}
