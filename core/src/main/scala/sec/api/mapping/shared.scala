/*
 * Copyright 2020 Scala Event Sourcing client for KurrentDB
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

import java.util.UUID as JUUID
import cats.{ApplicativeThrow, MonadThrow}
import cats.syntax.all.*
import com.eventstore.dbclient.proto.shared as ps
import com.google.protobuf.ByteString
import exceptions.{MaximumAppendSizeExceeded, StreamDeleted, WrongExpectedState}

private[sec] object shared:

  val mkUuid: JUUID => ps.UUID = j =>
    ps.UUID().withStructured(ps.UUID.Structured(j.getMostSignificantBits(), j.getLeastSignificantBits()))

  def mkJuuid[F[_]: ApplicativeThrow](uuid: ps.UUID): F[JUUID] =

    val juuid = uuid.value match
      case ps.UUID.Value.Structured(v) => new JUUID(v.mostSignificantBits, v.leastSignificantBits).asRight
      case ps.UUID.Value.String(v)     => Either.catchNonFatal(JUUID.fromString(v)).leftMap(_.getMessage())
      case ps.UUID.Value.Empty         => "UUID is missing".asLeft

    juuid.leftMap(ProtoResultError(_)).liftTo[F]

  //

  def mkStreamId[F[_]: MonadThrow](sid: Option[ps.StreamIdentifier]): F[StreamId] =
    mkStreamId[F](sid.getOrElse(ps.StreamIdentifier()))

  def mkStreamId[F[_]: MonadThrow](sid: ps.StreamIdentifier): F[StreamId] =
    sid.utf8[F] >>= { sidStr =>
      StreamId.stringToStreamId(sidStr).leftMap(ProtoResultError(_)).liftTo[F]
    }

  //

  def mkWrongExpectedStreamState[F[_]: MonadThrow](sid: StreamId, w: ps.WrongExpectedVersion): F[WrongExpectedState] =

    import ps.WrongExpectedVersion.ExpectedStreamPositionOption
    import ps.WrongExpectedVersion.CurrentStreamRevisionOption

    val expected: Either[Throwable, StreamState] = w.expectedStreamPositionOption match {
      case ExpectedStreamPositionOption.ExpectedStreamPosition(v) => StreamPosition(v).asRight
      case ExpectedStreamPositionOption.ExpectedNoStream(_)       => StreamState.NoStream.asRight
      case ExpectedStreamPositionOption.ExpectedAny(_)            => StreamState.Any.asRight
      case ExpectedStreamPositionOption.ExpectedStreamExists(_)   => StreamState.StreamExists.asRight
      case ExpectedStreamPositionOption.Empty                     => mkError("ExpectedStreamPositionOption is missing")
    }

    val actual: Either[Throwable, StreamState] = w.currentStreamRevisionOption match {
      case CurrentStreamRevisionOption.CurrentStreamRevision(v) => StreamPosition(v).asRight
      case CurrentStreamRevisionOption.CurrentNoStream(_)       => StreamState.NoStream.asRight
      case CurrentStreamRevisionOption.Empty                    => mkError("CurrentStreamRevisionOption is missing")
    }

    (expected, actual).mapN((e, a) => WrongExpectedState(sid, e, a)).liftTo[F]

  def mkStreamDeleted[F[_]: MonadThrow](sd: ps.StreamDeleted): F[StreamDeleted] =
    sd.streamIdentifier.traverse(_.utf8[F]).map(_.getOrElse("<unknown>")).map(StreamDeleted(_))

  def mkMaximumAppendSizeExceeded(e: ps.MaximumAppendSizeExceeded): MaximumAppendSizeExceeded =
    MaximumAppendSizeExceeded(e.maxAppendSize.some)

  ///

  def mkError[T](msg: String): Either[Throwable, T] =
    ProtoResultError(msg).asLeft[T]

  extension (v: StreamId)
    def esSid: ps.StreamIdentifier =
      v.stringValue.toStreamIdentifer

  extension (v: String)
    def toStreamIdentifer: ps.StreamIdentifier =
      ps.StreamIdentifier(ByteString.copyFromUtf8(v))

  extension (v: ps.StreamIdentifier)
    def utf8[F[_]](implicit F: ApplicativeThrow[F]): F[String] =
      F.catchNonFatal(Option(v.streamName.toStringUtf8()).getOrElse(""))

  extension (v: ps.AllStreamPosition)
    def toLogPosition: Either[InvalidInput, LogPosition.Exact] =
      LogPosition(v.commitPosition, v.preparePosition)
