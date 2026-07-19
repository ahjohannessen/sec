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

import com.google.protobuf.any.Any as PbAny
import com.google.rpc.Status as PbStatus
import io.grpc.{Metadata, Status, StatusRuntimeException}
import io.kurrentdb.protocol.v2.streams.errors as v2e
import sec.api.exceptions.*

class ConvertV2Suite extends SecSuite:

  private val detailsKey: Metadata.Key[Array[Byte]] =
    Metadata.Key.of("grpc-status-details-bin", Metadata.BINARY_BYTE_MARSHALLER)

  private def sre(details: PbAny*): StatusRuntimeException =
    val md = new Metadata()
    md.put(detailsKey, PbStatus(code = Status.Code.FAILED_PRECONDITION.value, details = details.toSeq).toByteArray)
    new StatusRuntimeException(Status.FAILED_PRECONDITION, md)

  test("consistency violation details map to AppendConsistencyViolation with per-stream info") {
    val d = v2e.AppendConsistencyViolationErrorDetails(violations =
      Seq(
        v2e.ConsistencyViolation(
          checkIndex = 1,
          `type`     = v2e.ConsistencyViolation.Type.StreamState(
            v2e.ConsistencyViolation.StreamStateViolation(stream = "s1", expectedState = -1L, actualState = 0L)
          )
        )
      )
    )
    assertEquals(
      convertV2.convertToEs(sre(PbAny.pack(d))),
      Some(v2.AppendConsistencyViolation(List(v2.AppendConsistencyViolation.StreamStateViolation("s1", -1L, 0L))))
    )
  }

  test("revision conflict, size exceeded and stream lifecycle details map to their exceptions") {
    val cases: List[(PbAny, EsException)] = List(
      PbAny.pack(v2e.StreamRevisionConflictErrorDetails("s", 3L, 5L)) -> v2.StreamRevisionConflict("s", 3L, 5L),
      PbAny.pack(v2e.AppendRecordSizeExceededErrorDetails("s", "r", 10, 5)) ->
        v2.AppendRecordSizeExceeded("s", "r", 10, 5),
      PbAny.pack(v2e.AppendTransactionSizeExceededErrorDetails(10, 5)) -> v2.AppendTransactionSizeExceeded(10, 5),
      PbAny.pack(v2e.StreamAlreadyExistsErrorDetails("s"))             -> v2.StreamAlreadyExists("s"),
      PbAny.pack(v2e.StreamTombstonedErrorDetails("s"))                -> v2.StreamTombstoned("s"),
      PbAny.pack(v2e.StreamDeletedErrorDetails("s"))                   -> StreamDeleted("s"),
      PbAny.pack(v2e.StreamNotFoundErrorDetails("s"))                  -> StreamNotFound("s")
    )
    cases.foreach { case (any, expected) =>
      assertEquals(convertV2.convertToEs(sre(any)), Some(expected), s"for ${any.typeUrl}")
    }
  }

  test("unrecognized or absent details yield None") {
    assertEquals(convertV2.convertToEs(sre()), None)
    assertEquals(convertV2.convertToEs(new StatusRuntimeException(Status.FAILED_PRECONDITION)), None)
    val foreign = PbAny.pack(v2e.StreamNotFoundErrorDetails("s")).copy(typeUrl = "type.googleapis.com/unknown.Type")
    assertEquals(convertV2.convertToEs(sre(foreign)), None)
  }
