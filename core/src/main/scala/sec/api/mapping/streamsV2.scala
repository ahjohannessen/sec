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
package mapping

import cats.data.NonEmptyList
import cats.syntax.all.*
import com.google.protobuf.struct as ps
import io.kurrentdb.protocol.v2.streams as pv2
import sec.api.v2.*

private[sec] object streamsV2:

  /** StreamState sentinel encoding used by v2 checks: -1 NoStream, -2 Any, -4 StreamExists,
    * otherwise the exact revision.
    */
  def mkExpectedState(ss: StreamState): Long = ss match
    case StreamState.NoStream     => -1L
    case StreamState.Any          => -2L
    case StreamState.StreamExists => -4L
    case e: StreamPosition.Exact  => e.value.toLong

  def mkSchemaFormat(f: SchemaFormat): pv2.SchemaFormat = f match
    case SchemaFormat.Json     => pv2.SchemaFormat.SCHEMA_FORMAT_JSON
    case SchemaFormat.Protobuf => pv2.SchemaFormat.SCHEMA_FORMAT_PROTOBUF
    case SchemaFormat.Avro     => pv2.SchemaFormat.SCHEMA_FORMAT_AVRO
    case SchemaFormat.Bytes    => pv2.SchemaFormat.SCHEMA_FORMAT_BYTES

  def mkValue(p: PropertyValue): ps.Value = p match
    case PropertyValue.Str(v)  => ps.Value(ps.Value.Kind.StringValue(v))
    case PropertyValue.Num(v)  => ps.Value(ps.Value.Kind.NumberValue(v))
    case PropertyValue.Bool(v) => ps.Value(ps.Value.Kind.BoolValue(v))

  def mkAppendRecord(streamId: StreamId.Id, r: Record): pv2.AppendRecord =
    pv2.AppendRecord(
      recordId   = Some(r.recordId.toString),
      properties = r.properties.toMap.view.mapValues(mkValue).toMap,
      schema     = Some(pv2.SchemaInfo(format = mkSchemaFormat(r.schema.format), name = r.schema.name)),
      data       = r.data.toByteString,
      stream     = streamId.stringValue
    )

  def mkCheck(streamId: StreamId.Id, expected: StreamState): pv2.ConsistencyCheck =
    pv2
      .ConsistencyCheck()
      .withStreamState(
        pv2.ConsistencyCheck.StreamStateCheck(stream = streamId.stringValue, expectedState = mkExpectedState(expected))
      )

  /** Every written stream carries exactly one check; guards add checks for unwritten streams. */
  def mkAppendRecordsRequest(appends: NonEmptyList[StreamAppend], guards: List[StreamGuard]): pv2.AppendRecordsRequest =
    pv2.AppendRecordsRequest(
      records = appends.toList.flatMap(a => a.records.toList.map(mkAppendRecord(a.streamId, _))),
      checks  = appends.toList.map(a => mkCheck(a.streamId, a.expected)) ++
        guards.map(g => mkCheck(g.streamId, g.expected))
    )

  /** Recovers typed stream ids by joining response revisions with the request's appends. */
  def mkMultiAppendResult(
    appends: NonEmptyList[StreamAppend]
  )(resp: pv2.AppendRecordsResponse): Attempt[MultiAppendResult] =
    val byName = appends.toList.map(a => a.streamId.stringValue -> a.streamId).toMap
    resp.revisions.toList
      .traverse { r =>
        byName
          .get(r.stream)
          .toRight(s"Unexpected stream '${r.stream}' in AppendRecordsResponse")
          .map(id => id -> StreamPosition(r.revision))
      }
      .flatMap(rs => NonEmptyList.fromList(rs).toRight("AppendRecordsResponse contained no revisions"))
      .map(MultiAppendResult(resp.position, _))
