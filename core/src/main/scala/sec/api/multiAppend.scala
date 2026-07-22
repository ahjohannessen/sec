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

import java.util.UUID
import cats.data.NonEmptyList
import scodec.bits.ByteVector

/** A record for [[Streams.multiStreamAppend]]. Unlike [[EventData]] there are no raw metadata
  * bytes: the server synthesizes the v1 metadata view as a JSON object of [[RecordData.properties]]
  * merged with reserved `$`-prefixed schema keys, and [[Schema.name]] surfaces as the v1 eventType.
  */
case class RecordData(
  recordId: UUID,
  schema: Schema,
  data: ByteVector,
  properties: Properties
)

case class Schema(name: String, format: SchemaFormat)

object Schema:
  def json(name: String): Schema = Schema(name, SchemaFormat.Json)

enum SchemaFormat:
  case Json, Protobuf, Avro, Bytes

enum PropertyValue:
  case Null
  case Str(value: String)
  case Num(value: Double)
  case Bool(value: Boolean)
  case Arr(values: List[PropertyValue])
  case Obj(fields: Properties)

/** Validated user properties. Keys must be non-empty and must not use the reserved `$` prefix -
  * the server owns that namespace (`$schema.name`, `$schema.format`, ...).
  */
case class Properties private (toMap: Map[String, PropertyValue]):
  def isEmpty: Boolean = toMap.isEmpty

object Properties:

  val empty: Properties = new Properties(Map.empty)

  def of(kvs: (String, PropertyValue)*): Either[String, Properties] =
    val bad = kvs.collect { case (k, _) if k.isEmpty || k.startsWith("$") => k }
    if bad.isEmpty then Right(new Properties(kvs.toMap))
    else Left(s"Invalid property keys (empty or reserved '$$' prefix): ${bad.mkString(", ")}")

/** Append to one stream with a mandatory expectation: unlike the raw protocol, where records and
  * checks are independent lists, a check is always attached to every written stream.
  */
case class StreamAppend(
  streamId: StreamId.Id,
  expected: StreamState,
  records: NonEmptyList[RecordData]
)

/** A consistency condition on a stream that is not written to (dynamic consistency boundary):
  * the whole append succeeds only if the guarded stream satisfies [[StreamGuard.expected]].
  */
case class StreamGuard(streamId: StreamId.Id, expected: StreamState)

/** @param position
  *   the log position of the committed transaction. The v2 protocol reports a single position,
  *   unlike v1's commit / prepare pair; it is mapped as commit == prepare.
  */
case class MultiAppendResult(
  position: LogPosition.Exact,
  streamPositions: NonEmptyList[(StreamId.Id, StreamPosition.Exact)]
)
