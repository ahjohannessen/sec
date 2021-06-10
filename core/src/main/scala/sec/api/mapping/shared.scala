/*
 * Copyright 2020 Scala EventStoreDB Client
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sec
package api
package mapping

import java.util.{UUID => JUUID}
import cats.{ApplicativeThrow, MonadThrow}
import cats.syntax.all._
import com.eventstore.client.{StreamIdentifier, UUID}
import com.google.protobuf.ByteString
import sec.StreamId

private[sec] object shared {

  val mkUuid: JUUID => UUID = j =>
    UUID().withStructured(UUID.Structured(j.getMostSignificantBits(), j.getLeastSignificantBits()))

  def mkJuuid[F[_]: ApplicativeThrow](uuid: UUID): F[JUUID] = {

    val juuid = uuid.value match {
      case UUID.Value.Structured(v) => new JUUID(v.mostSignificantBits, v.leastSignificantBits).asRight
      case UUID.Value.String(v)     => Either.catchNonFatal(JUUID.fromString(v)).leftMap(_.getMessage())
      case UUID.Value.Empty         => "UUID is missing".asLeft
    }

    juuid.leftMap(ProtoResultError(_)).liftTo[F]
  }

  //

  def mkStreamId[F[_]: MonadThrow](sid: Option[StreamIdentifier]): F[StreamId] =
    mkStreamId[F](sid.getOrElse(StreamIdentifier()))

  def mkStreamId[F[_]: MonadThrow](sid: StreamIdentifier): F[StreamId] =
    sid.utf8[F] >>= { sidStr =>
      StreamId.stringToStreamId(sidStr).leftMap(ProtoResultError(_)).liftTo[F]
    }

  implicit final class StreamIdOps(val v: StreamId) extends AnyVal {
    def esSid: StreamIdentifier = v.stringValue.toStreamIdentifer
  }

  implicit final class StringOps(val v: String) extends AnyVal {
    def toStreamIdentifer: StreamIdentifier = StreamIdentifier(ByteString.copyFromUtf8(v))
  }

  implicit final class StreamIdentifierOps(val v: StreamIdentifier) extends AnyVal {
    def utf8[F[_]](implicit F: ApplicativeThrow[F]): F[String] =
      F.catchNonFatal(Option(v.streamName.toStringUtf8()).getOrElse(""))
  }

  //

}
