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
import com.eventstore.{client => cp}
import com.google.protobuf.ByteString
import sec.StreamId
import exceptions.{MaximumAppendSizeExceeded, StreamDeleted, WrongExpectedState}

private[sec] object shared {

  val mkUuid: JUUID => cp.UUID = j =>
    cp.UUID().withStructured(cp.UUID.Structured(j.getMostSignificantBits(), j.getLeastSignificantBits()))

  def mkJuuid[F[_]: ApplicativeThrow](uuid: cp.UUID): F[JUUID] = {

    val juuid = uuid.value match {
      case cp.UUID.Value.Structured(v) => new JUUID(v.mostSignificantBits, v.leastSignificantBits).asRight
      case cp.UUID.Value.String(v)     => Either.catchNonFatal(JUUID.fromString(v)).leftMap(_.getMessage())
      case cp.UUID.Value.Empty         => "UUID is missing".asLeft
    }

    juuid.leftMap(ProtoResultError(_)).liftTo[F]
  }

  //

  def mkStreamId[F[_]: MonadThrow](sid: Option[cp.StreamIdentifier]): F[StreamId] =
    mkStreamId[F](sid.getOrElse(cp.StreamIdentifier()))

  def mkStreamId[F[_]: MonadThrow](sid: cp.StreamIdentifier): F[StreamId] =
    sid.utf8[F] >>= { sidStr =>
      StreamId.stringToStreamId(sidStr).leftMap(ProtoResultError(_)).liftTo[F]
    }

  //

  def mkWrongExpectedStreamState[F[_]: MonadThrow](sid: StreamId, w: cp.WrongExpectedVersion): F[WrongExpectedState] = {

    import cp.WrongExpectedVersion.ExpectedStreamPositionOption
    import cp.WrongExpectedVersion.CurrentStreamRevisionOption

    val expected: Either[Throwable, StreamState] = w.expectedStreamPositionOption match {
      case ExpectedStreamPositionOption.ExpectedStreamPosition(v) => StreamPosition.exact(v).asRight
      case ExpectedStreamPositionOption.ExpectedNoStream(_)       => StreamState.NoStream.asRight
      case ExpectedStreamPositionOption.ExpectedAny(_)            => StreamState.Any.asRight
      case ExpectedStreamPositionOption.ExpectedStreamExists(_)   => StreamState.StreamExists.asRight
      case ExpectedStreamPositionOption.Empty                     => mkError("ExpectedStreamPositionOption is missing")
    }

    val actual: Either[Throwable, StreamState] = w.currentStreamRevisionOption match {
      case CurrentStreamRevisionOption.CurrentStreamRevision(v) => StreamPosition.exact(v).asRight
      case CurrentStreamRevisionOption.CurrentNoStream(_)       => StreamState.NoStream.asRight
      case CurrentStreamRevisionOption.Empty                    => mkError("CurrentStreamRevisionOption is missing")
    }

    (expected, actual).mapN((e, a) => WrongExpectedState(sid, e, a)).liftTo[F]
  }

  def mkStreamDeleted[F[_]: MonadThrow](sd: cp.StreamDeleted): F[StreamDeleted] =
    sd.streamIdentifier.traverse(_.utf8[F]).map(_.getOrElse("<unknown>")).map(StreamDeleted(_))

  def mkMaximumAppendSizeExceeded(e: cp.MaximumAppendSizeExceeded): MaximumAppendSizeExceeded =
    MaximumAppendSizeExceeded(e.maxAppendSize.some)

  ///

  def mkError[T](msg: String): Either[Throwable, T] =
    ProtoResultError(msg).asLeft[T]

  implicit final class StreamIdOps(val v: StreamId) extends AnyVal {
    def esSid: cp.StreamIdentifier = v.stringValue.toStreamIdentifer
  }

  implicit final class StringOps(val v: String) extends AnyVal {
    def toStreamIdentifer: cp.StreamIdentifier = cp.StreamIdentifier(ByteString.copyFromUtf8(v))
  }

  implicit final class StreamIdentifierOps(val v: cp.StreamIdentifier) extends AnyVal {
    def utf8[F[_]](implicit F: ApplicativeThrow[F]): F[String] =
      F.catchNonFatal(Option(v.streamName.toStringUtf8()).getOrElse(""))
  }

  //

}
