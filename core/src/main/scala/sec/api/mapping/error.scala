/*
 * Copyright 2020 Alex Henning Johannessen
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

//======================================================================================================================

final private[sec] case class EncodingError(msg: String) extends RuntimeException(msg)
private[sec] object EncodingError {
  def apply(e: Throwable): EncodingError = EncodingError(e.getMessage)
}

//======================================================================================================================

final private[sec] case class DecodingError(msg: String) extends RuntimeException(msg)
private[sec] object DecodingError {
  def apply(e: Throwable): DecodingError = DecodingError(e.getMessage)
}

//======================================================================================================================

final private[sec] case class ProtoResultError(msg: String) extends RuntimeException(msg)

//======================================================================================================================
