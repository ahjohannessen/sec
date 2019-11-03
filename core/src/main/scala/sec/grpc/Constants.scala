package sec
package grpc

object Constants {

  object Exceptions {

    val ExceptionKey: String = "exception"

    val AccessDenied: String         = "access-denied"
    val InvalidTransaction: String   = "invalid-transaction"
    val StreamDeleted: String        = "stream-deleted"
    val WrongExpectedVersion: String = "wrong-expected-version"
    val NotFound: String             = "not-found"

    val PersistentSubscriptionFailed: String       = "persistent-subscription-failed"
    val PersistentSubscriptionDoesNotExist: String = "persistent-subscription-does-not-exist"
    val PersistentSubscriptionExists: String       = "persistent-subscription-exists"
    val MaximumSubscribersReached: String          = "maximum-subscribers-reached"
    val PersistentSubscriptionDropped: String      = "persistent-subscription-dropped"

    val ExpectedVersion: String = "expected-version"
    val ActualVersion: String   = "actual-version"
    val StreamName: String      = "stream-name"
    val GroupName: String       = "group-name"
    val Reason: String          = "reason"
  }

  object Metadata {
    val IsJson: String  = "is-json"
    val Type: String    = "type"
    val Created: String = "created"
  }

  object Headers {
    val Authorization: String = "authorization"
    val BasicScheme: String   = "Basic"
  }

}
