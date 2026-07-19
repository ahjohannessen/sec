/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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
package grpc

import cats.syntax.all.*
import com.google.protobuf.any.Any as PbAny
import com.google.rpc.Status as PbStatus
import io.grpc.{Metadata, StatusRuntimeException}
import io.kurrentdb.protocol.v2.streams.errors as v2e
import sec.api.exceptions.*

/** Converts v2 protocol errors to typed exceptions. Unlike v1, which uses bespoke string trailers,
  * v2 carries structured details: a google.rpc.Status in the standard `grpc-status-details-bin`
  * trailer, holding packed messages from `kurrentdb/protocol/v2/streams/errors.proto`.
  */
private[sec] object convertV2:

  private val statusDetails: Metadata.Key[Array[Byte]] =
    Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER)

  /** None when no recognized structured details are present - callers fall back to status-code
    * level handling.
    */
  val convertToEs: StatusRuntimeException => Option[EsException] = ex =>
    for
      md     <- Option(ex.getTrailers)
      bytes  <- Option(md.get(statusDetails))
      status <- Either.catchNonFatal(PbStatus.parseFrom(bytes)).toOption
      es     <- status.details.toList.collectFirstSome(fromDetail)
    yield es

  private def unpackAs[A <: scalapb.GeneratedMessage: scalapb.GeneratedMessageCompanion](a: PbAny): Option[A] =
    if a.is[A] then Either.catchNonFatal(a.unpack[A]).toOption else None

  private def fromDetail(a: PbAny): Option[EsException] =
    unpackAs[v2e.AppendConsistencyViolationErrorDetails](a).map { d =>
      AppendConsistencyViolation(d.violations.toList.flatMap { cv =>
        cv.`type`.streamState.map { ss =>
          AppendConsistencyViolation.StreamStateViolation(ss.stream, ss.expectedState, ss.actualState)
        }
      })
    } orElse
      unpackAs[v2e.StreamRevisionConflictErrorDetails](a).map { d =>
        StreamRevisionConflict(d.stream, d.expectedRevision, d.actualRevision)
      } orElse
      unpackAs[v2e.AppendRecordSizeExceededErrorDetails](a).map { d =>
        AppendRecordSizeExceeded(d.stream, d.recordId, d.size, d.maxSize)
      } orElse
      unpackAs[v2e.AppendTransactionSizeExceededErrorDetails](a).map { d =>
        AppendTransactionSizeExceeded(d.size, d.maxSize)
      } orElse
      unpackAs[v2e.StreamAlreadyExistsErrorDetails](a).map(d => StreamAlreadyExists(d.stream)) orElse
      unpackAs[v2e.StreamTombstonedErrorDetails](a).map(d => StreamTombstoned(d.stream)) orElse
      unpackAs[v2e.StreamDeletedErrorDetails](a).map(d => StreamDeleted(d.stream)) orElse
      unpackAs[v2e.StreamNotFoundErrorDetails](a).map(d => StreamNotFound(d.stream))
