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

import cats.ApplicativeThrow
import cats.data.NonEmptyList
import cats.syntax.all.*
import com.google.protobuf.struct as ps
import io.kurrentdb.protocol.v2.streams as pv2

private[sec] object streamsV2:

  /** StreamState sentinel encoding used by v2 checks: -1 NoStream, -4 StreamExists, otherwise the
    * exact stream position. The server also knows Deleted (-5) and Tombstoned (-6), which the sec
    * model does not yet express; -2 (Any) exists in the v1 vocabulary only and is rejected by the
    * server - Any is expressed by omitting the check, see [[mkAppendRecordsRequest]].
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
    case PropertyValue.Null    => ps.Value(ps.Value.Kind.NullValue(ps.NullValue.NULL_VALUE))
    case PropertyValue.Str(v)  => ps.Value(ps.Value.Kind.StringValue(v))
    case PropertyValue.Num(v)  => ps.Value(ps.Value.Kind.NumberValue(v))
    case PropertyValue.Bool(v) => ps.Value(ps.Value.Kind.BoolValue(v))
    case PropertyValue.Arr(vs) => ps.Value(ps.Value.Kind.ListValue(ps.ListValue(vs.map(mkValue))))
    case PropertyValue.Obj(fs) => ps.Value(ps.Value.Kind.StructValue(ps.Struct(fs.toMap.view.mapValues(mkValue).toMap)))

  def mkAppendRecord(streamId: StreamId.Id, r: RecordData): pv2.AppendRecord =
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

  /** Every written stream carries at most one check; guards add checks for unwritten streams.
    * The v2 protocol has no Any sentinel - the server rejects -2 with INVALID_ARGUMENT - so Any
    * is expressed by omitting the check, for both appends and guards.
    */
  def mkAppendRecordsRequest(appends: NonEmptyList[StreamAppend], guards: List[StreamGuard]): pv2.AppendRecordsRequest =
    pv2.AppendRecordsRequest(
      records = appends.toList.flatMap(a => a.records.toList.map(mkAppendRecord(a.streamId, _))),
      checks  = appends.toList.collect { case a if a.expected != StreamState.Any => mkCheck(a.streamId, a.expected) } ++
        guards.collect { case g if g.expected != StreamState.Any => mkCheck(g.streamId, g.expected) }
    )

  /** Recovers typed stream ids by joining response revisions with the request's appends. */
  def mkMultiAppendResult[F[_]: ApplicativeThrow](
    appends: NonEmptyList[StreamAppend]
  )(resp: pv2.AppendRecordsResponse): F[MultiAppendResult] =
    val byName = appends.toList.map(a => a.streamId.stringValue -> a.streamId).toMap
    resp.revisions.toList
      .traverse { r =>
        byName
          .get(r.stream)
          .map(id => id -> StreamPosition(r.revision))
          .fold(shared.mkError[(StreamId.Id, StreamPosition.Exact)](
            s"Unexpected stream '${r.stream}' in AppendRecordsResponse"
          ))(_.asRight)
      }
      .flatMap { rs =>
        NonEmptyList.fromList(rs).fold(shared.mkError[NonEmptyList[(StreamId.Id, StreamPosition.Exact)]](
          "AppendRecordsResponse contained no stream positions"
        ))(_.asRight)
      }
      .flatMap { rs =>
        val missing = appends.toList.map(_.streamId.stringValue).toSet -- rs.toList.map(_._1.stringValue).toSet
        if missing.isEmpty then rs.asRight
        else shared.mkError[NonEmptyList[(StreamId.Id, StreamPosition.Exact)]](
          s"AppendRecordsResponse missing stream positions for: ${missing.mkString(", ")}"
        )
      }
      .map(MultiAppendResult(LogPosition.exact(resp.position, resp.position), _))
      .liftTo[F]
