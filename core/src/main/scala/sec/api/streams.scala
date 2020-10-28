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

/**
 * Checkpoint result used with server-side filtering in EventStoreDB.
 * Contains the [[LogPosition.Exact]] when the checkpoint was made.
 */
final case class Checkpoint(
  logPosition: LogPosition.Exact
)

/**
 * The current last [[StreamPosition.Exact]] of the stream
 * appended to and its corresponding [[LogPosition.Exact]] in the transaction log.
 */
final case class WriteResult(
  streamPosition: StreamPosition.Exact,
  logPosition: LogPosition.Exact
)

/**
 * The [[LogPosition.Exact]] of the delete in the transaction log.
 */
final case class DeleteResult(
  logPosition: LogPosition.Exact
)

/**
 * The [[LogPosition.Exact]] of the tombstone in the transaction log.
 */
final case class TombstoneResult(
  logPosition: LogPosition.Exact
)

/**
 * A subscription confirmation identifier as the first value from
 * the EventStoreDB when subscribing to a stream. Not intended for
 * public use.
 */
final private[sec] case class SubscriptionConfirmation(
  id: String
)
