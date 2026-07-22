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

import cats.data.NonEmptyList
import scala.util.control.NoStackTrace

object exceptions:

  sealed abstract class EsException private[sec] (
    msg: String,
    cause: Option[Throwable] = None
  ) extends RuntimeException(msg, cause.orNull)
    with NoStackTrace

  case object AccessDenied extends EsException("Access Denied.")
  case object InvalidTransaction extends EsException("Invalid Transaction.")
  case class UserNotFound(loginName: String) extends EsException(s"User '$loginName' was not found.")
  case class StreamDeleted(streamId: String) extends EsException(s"Event stream '$streamId' is deleted.")
  case class StreamNotFound(streamId: String) extends EsException(s"Event stream '$streamId' was not found.")
  case class UnknownError(msg: String) extends EsException(msg)
  case class ServerUnavailable(msg: String) extends EsException(msg)
  case class ResubscriptionRequired(msg: String) extends EsException(msg)

  case class SubscriptionPoolExhausted(channels: Int)
    extends EsException(s"All $channels pooled subscription channels are at capacity - refusing to queue silently.")

  case class NotLeader(
    host: Option[String],
    port: Option[Int]
  ) extends EsException(NotLeader.msg(host, port))

  object NotLeader:
    def msg(host: Option[String], port: Option[Int]): String =
      s"Not leader. Leader at ${host.getOrElse("<unknown>")}:${port.getOrElse("<unknown>")}."

  case class MaximumAppendSizeExceeded(size: Option[Int]) extends EsException(MaximumAppendSizeExceeded.msg(size))

  object MaximumAppendSizeExceeded:
    def msg(maxSize: Option[Int]): String =
      s"Maximum append size ${maxSize.map(max => s"of $max bytes ").getOrElse("")}exceeded."

  case class WrongExpectedState(
    sid: StreamId,
    expected: StreamState,
    actual: StreamState
  ) extends EsException(WrongExpectedState.msg(sid, expected, actual))

  object WrongExpectedState:

    def msg(sid: StreamId, expected: StreamState, actual: StreamState): String =
      s"Wrong expected state for stream: ${sid.render}, expected: ${expected.render}, actual: ${actual.render}"

  // TODO: Remove when ESDB does not transport this via response headers
  case class WrongExpectedVersion(
    streamId: String,
    expected: Option[Long],
    actual: Option[Long]
  ) extends EsException(WrongExpectedVersion.msg(streamId, expected, actual))

  private[sec] object WrongExpectedVersion:

    def msg(streamId: String, expected: Option[Long], actual: Option[Long]): String =
      val exp = expected.map(_.toString).getOrElse("<unknown>")
      val act = actual.map(_.toString).getOrElse("<unknown>")
      s"WrongExpectedVersion for stream: $streamId, expected version: $exp, actual version: $act"

    extension (e: WrongExpectedVersion)
      def adaptOrFallback(sid: StreamId, expected: StreamState): Throwable =
        def mkException(actual: StreamState) = WrongExpectedState(sid, expected, actual)
        def noStream                         = mkException(StreamState.NoStream)
        def mkExact(a: Long)                 = if (a == -1) noStream else mkException(StreamPosition(a))

        e.actual.fold(noStream)(mkExact)

  object PersistentSubscription:

    case class Failed(streamId: String, groupName: String, reason: String)
      extends EsException(s"Subscription group $groupName on stream $streamId failed: '$reason'.")

    case class Exists(streamId: String, groupName: String)
      extends EsException(s"Subscription group $groupName on stream $streamId exists.")

    case class NotFound(streamId: String, groupName: String)
      extends EsException(s"Subscription group '$groupName' on stream '$streamId' does not exist.")

    case class Dropped(streamId: String, groupName: String)
      extends EsException(s"Subscription group '$groupName' on stream '$streamId' was dropped by server.")

    case class MaximumSubscribersReached(streamId: String, groupName: String)
      extends EsException(s"Maximum subscriptions reached for subscription group '$groupName' on stream '$streamId.'")

  // Multi-stream append. Constructed from structured google.rpc.Status details of the v2
  // protocol, see [[sec.api.grpc.convertV2]].

  case class StreamAlreadyExists(streamId: String)
    extends EsException(s"Event stream '$streamId' already exists.")

  case class StreamTombstoned(streamId: String)
    extends EsException(s"Event stream '$streamId' is tombstoned.")

  case class StreamConditionMismatch(streamId: String, expected: ExpectedCondition, actual: ActualCondition)
    extends EsException(s"Stream '$streamId' condition mismatch: expected ${expected.render}, actual ${actual.render}.")

  case class AppendRecordSizeExceeded(streamId: String, recordId: String, size: Int, maxSize: Int)
    extends EsException(s"Record '$recordId' for stream '$streamId' is $size bytes, exceeding max of $maxSize.")

  case class AppendTransactionSizeExceeded(size: Int, maxSize: Int)
    extends EsException(s"Append transaction is $size bytes, exceeding max of $maxSize.")

  case class FieldViolation(field: String, description: String)

  case class InvalidRequest(violations: NonEmptyList[FieldViolation])
    extends EsException(InvalidRequest.mkMsg(violations))

  object InvalidRequest:
    private[sec] def mkMsg(vs: NonEmptyList[FieldViolation]): String =
      vs.toList
        .map(v => if v.field.isEmpty then v.description else s"${v.field}: ${v.description}")
        .mkString("Request validation failed: ", "; ", ".")

  case class AppendConsistencyViolation(violations: List[AppendConsistencyViolation.StreamConditionViolation])
    extends EsException(AppendConsistencyViolation.mkMsg(violations))

  object AppendConsistencyViolation:

    case class StreamConditionViolation(streamId: String, expected: ExpectedCondition, actual: ActualCondition)

    private[sec] def mkMsg(vs: List[StreamConditionViolation]): String =
      val detail = vs.map(v => s"'${v.streamId}' expected ${v.expected.render}, actual ${v.actual.render}").mkString("; ")
      s"Append failed due to consistency violations: $detail."
