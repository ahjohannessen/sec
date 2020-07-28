package sec
package core

import EventNumber.Exact

sealed abstract class EsException(msg: String) extends RuntimeException(msg)

case object AccessDenied                          extends EsException("Access Denied.")
case object InvalidTransaction                    extends EsException("Invalid Transaction.")
final case class UserNotFound(loginName: String)  extends EsException(s"User '$loginName' was not found.")
final case class StreamDeleted(streamId: String)  extends EsException(s"Event stream '$streamId' is deleted.")
final case class StreamNotFound(streamId: String) extends EsException(s"Event stream '$streamId' was not found.")
final case class UnknownError(msg: String)        extends EsException(msg)
final case class ServerUnavailable(msg: String)   extends EsException(msg)
final case class ValidationError(msg: String)     extends EsException(msg)

final case class NotLeader(
  host: Option[String],
  port: Option[Int]
) extends EsException(NotLeader.msg(host, port))

object NotLeader {
  def msg(host: Option[String], port: Option[Int]): String =
    s"Not leader. New leader at ${host.getOrElse("<unknown>")}:${port.getOrElse("<unknown>")}."
}

final case class MaximumAppendSizeExceeded(size: Option[Int]) extends EsException(MaximumAppendSizeExceeded.msg(size))

object MaximumAppendSizeExceeded {
  def msg(maxSize: Option[Int]): String =
    s"Maximum append size ${maxSize.map(max => s"of $max bytes ").getOrElse("")}exceeded."
}

final case class WrongExpectedVersion(
  streamId: String,
  expected: Option[Long],
  actual: Option[Long]
) extends EsException(WrongExpectedVersion.msg(streamId, expected, actual))

object WrongExpectedVersion {

  def apply(sid: StreamId, expected: Option[Exact], actual: Option[Exact]): WrongExpectedVersion =
    WrongExpectedVersion(sid.stringValue, expected.map(_.value), actual.map(_.value))

  def msg(streamId: String, expected: Option[Long], actual: Option[Long]): String = {
    val exp = expected.map(_.toString).getOrElse("<unknown>")
    val act = actual.map(_.toString).getOrElse("<unknown>")
    s"WrongExpectedVersion for stream: $streamId, expected version: $exp, actual version: $act"
  }
}

final case class PersistentSubscriptionFailed(streamId: String, groupName: String, reason: String)
  extends EsException(s"Subscription group $groupName on stream $streamId failed: '$reason'.")

final case class PersistentSubscriptionExists(streamId: String, groupName: String)
  extends EsException(s"Subscription group $groupName on stream $streamId exists.")

final case class PersistentSubscriptionNotFound(streamId: String, groupName: String)
  extends EsException(s"Subscription group '$groupName' on stream '$streamId' does not exist.")

final case class PersistentSubscriptionDroppedByServer(streamId: String, groupName: String)
  extends EsException(s"Subscription group '$groupName' on stream '$streamId' was dropped.")

final case class PersistentSubscriptionMaximumSubscribersReached(streamId: String, groupName: String)
  extends EsException(s"Maximum subscriptions reached for subscription group '$groupName' on stream '$streamId.'")
