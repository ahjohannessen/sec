package sec
package api
package grpc

private[api] object constants {

//======================================================================================================================

  object Exceptions {

    val ExceptionKey: String                       = "exception"
    val AccessDenied: String                       = "access-denied"
    val InvalidTransaction: String                 = "invalid-transaction"
    val StreamDeleted: String                      = "stream-deleted"
    val WrongExpectedVersion: String               = "wrong-expected-version"
    val StreamNotFound: String                     = "stream-not-found"
    val MaximumAppendSizeExceeded: String          = "maximum-append-size-exceeded"
    val NotLeader: String                          = "not-leader"
    val PersistentSubscriptionFailed: String       = "persistent-subscription-failed"
    val PersistentSubscriptionDoesNotExist: String = "persistent-subscription-does-not-exist"
    val PersistentSubscriptionExists: String       = "persistent-subscription-exists"
    val MaximumSubscribersReached: String          = "maximum-subscribers-reached"
    val PersistentSubscriptionDropped: String      = "persistent-subscription-dropped"
    val UserNotFound: String                       = "user-not-found"
    val UserConflict: String                       = "user-conflict"
    val ScavengeNotFound: String                   = "scavenge-not-found"

    val ExpectedVersion: String   = "expected-version"
    val ActualVersion: String     = "actual-version"
    val StreamName: String        = "stream-name"
    val GroupName: String         = "group-name"
    val Reason: String            = "reason"
    val MaximumAppendSize: String = "maximum-append-size"
    val ScavengeId: String        = "scavenge-id"
    val LeaderEndpoint: String    = "leader-endpoint"

    val LoginName = "login-name"
  }

//======================================================================================================================

  object Metadata {

    val ContentType: String = "content-type"
    val Type: String        = "type"
    val Created: String     = "created"

    object ContentTypes {
      val ApplicationJson: String        = "application/json"
      val ApplicationOctetStream: String = "application/octet-stream"
    }

  }

//======================================================================================================================

  object Headers {
    val Authorization: String  = "authorization"
    val BasicScheme: String    = "Basic"
    val ConnectionName: String = "connection-name"
    val RequiresLeader: String = "requires-leader"
  }

//======================================================================================================================

}
