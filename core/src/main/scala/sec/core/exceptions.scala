package sec
package core

sealed abstract class EsException(msg: String) extends RuntimeException(msg)

case object AccessDenied                          extends EsException("Access Denied.") // All
case object InvalidTransaction                    extends EsException("Invalid Transaction.") // Streams.Delete + Streams.Append
final case class UserNotFound(loginName: String)  extends EsException(s"User '$loginName' was not found.") // Users
final case class StreamDeleted(streamId: String)  extends EsException(s"Event stream '$streamId' is deleted.") // Streams
final case class StreamNotFound(streamId: String) extends EsException(s"Event stream '$streamId' was not found.") // Streams.Read/Subscribe
final case class UnknownError(msg: String)        extends EsException(msg)
final case class ServerUnavailable(msg: String)   extends EsException(msg)

final case class MaximumAppendSizeExceeded(size: Option[Int]) extends EsException(MaximumAppendSizeExceeded.msg(size)) // Streams.Append

object MaximumAppendSizeExceeded {
  def msg(maxSize: Option[Int]): String =
    s"Maximum append size ${maxSize.map(max => s"of $max bytes ").getOrElse("")}exceeded."
}

final case class WrongExpectedVersion(streamId: String, expected: Option[Long], actual: Option[Long]) // Streams.Delete + Streams.Append
  extends EsException(WrongExpectedVersion.msg(streamId, expected, actual))

object WrongExpectedVersion {
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
