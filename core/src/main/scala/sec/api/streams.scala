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

final case class Checkpoint(logPosition: LogPosition.Exact)
final case class WriteResult(streamPosition: StreamPosition.Exact, logPosition: LogPosition.Exact)
final case class DeleteResult(logPosition: LogPosition.Exact)

final private[sec] case class SubscriptionConfirmation(id: String)
