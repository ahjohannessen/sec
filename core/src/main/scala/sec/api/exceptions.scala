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

import scala.util.control.NoStackTrace

import cats.syntax.all._

object exceptions {

  sealed abstract class EsException private[sec] (
    msg: String,
    cause: Option[Throwable] = None
  ) extends RuntimeException(msg, cause.orNull)
    with NoStackTrace

  case object AccessDenied                          extends EsException("Access Denied.")
  case object InvalidTransaction                    extends EsException("Invalid Transaction.")
  final case class UserNotFound(loginName: String)  extends EsException(s"User '$loginName' was not found.")
  final case class StreamDeleted(streamId: String)  extends EsException(s"Event stream '$streamId' is deleted.")
  final case class StreamNotFound(streamId: String) extends EsException(s"Event stream '$streamId' was not found.")
  final case class UnknownError(msg: String)        extends EsException(msg)
  final case class ServerUnavailable(msg: String)   extends EsException(msg)

  final case class NotLeader(
    host: Option[String],
    port: Option[Int]
  ) extends EsException(NotLeader.msg(host, port))

  object NotLeader {
    def msg(host: Option[String], port: Option[Int]): String =
      s"Not leader. Leader at ${host.getOrElse("<unknown>")}:${port.getOrElse("<unknown>")}."
  }

  final case class MaximumAppendSizeExceeded(size: Option[Int]) extends EsException(MaximumAppendSizeExceeded.msg(size))

  object MaximumAppendSizeExceeded {
    def msg(maxSize: Option[Int]): String =
      s"Maximum append size ${maxSize.map(max => s"of $max bytes ").getOrElse("")}exceeded."
  }

  final case class WrongExpectedState(
    sid: StreamId,
    expected: StreamState,
    actual: StreamState
  ) extends EsException(WrongExpectedState.msg(sid, expected, actual))

  object WrongExpectedState {

    def msg(sid: StreamId, expected: StreamState, actual: StreamState): String =
      s"Wrong expected state for stream: ${sid.show}, expected: ${expected.show}, actual: ${actual.show}"

  }

  // TODO: Remove when ESDB does not transport this via response headers
  final case class WrongExpectedVersion(
    streamId: String,
    expected: Option[Long],
    actual: Option[Long]
  ) extends EsException(WrongExpectedVersion.msg(streamId, expected, actual))

  private[sec] object WrongExpectedVersion {

    def msg(streamId: String, expected: Option[Long], actual: Option[Long]): String = {
      val exp = expected.map(_.toString).getOrElse("<unknown>")
      val act = actual.map(_.toString).getOrElse("<unknown>")
      s"WrongExpectedVersion for stream: $streamId, expected version: $exp, actual version: $act"
    }

    def adaptOrFallback(e: WrongExpectedVersion, sid: StreamId, expected: StreamState): Throwable = {

      def mkException(actual: StreamState) = WrongExpectedState(sid, expected, actual)
      def noStream                         = mkException(StreamState.NoStream).some
      def mkExact(a: Long)                 = StreamPosition.Exact(a).toOption.map(mkException)

      e.actual.fold(noStream)(mkExact).getOrElse(e)
    }

    implicit final class WrongExpectedVersionOps(val e: WrongExpectedVersion) extends AnyVal {
      def adaptOrFallback(sid: StreamId, expected: StreamState): Throwable =
        WrongExpectedVersion.adaptOrFallback(e, sid, expected)
    }

  }

  object PersistentSubscription {

    final case class Failed(streamId: String, groupName: String, reason: String)
      extends EsException(s"Subscription group $groupName on stream $streamId failed: '$reason'.")

    final case class Exists(streamId: String, groupName: String)
      extends EsException(s"Subscription group $groupName on stream $streamId exists.")

    final case class NotFound(streamId: String, groupName: String)
      extends EsException(s"Subscription group '$groupName' on stream '$streamId' does not exist.")

    final case class Dropped(streamId: String, groupName: String)
      extends EsException(s"Subscription group '$groupName' on stream '$streamId' was dropped by server.")

    final case class MaximumSubscribersReached(streamId: String, groupName: String)
      extends EsException(s"Maximum subscriptions reached for subscription group '$groupName' on stream '$streamId.'")

  }

}
