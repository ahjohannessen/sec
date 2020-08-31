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

    val ExpectedVersion: String    = "expected-version"
    val ActualVersion: String      = "actual-version"
    val StreamName: String         = "stream-name"
    val GroupName: String          = "group-name"
    val Reason: String             = "reason"
    val MaximumAppendSize: String  = "maximum-append-size"
    val ScavengeId: String         = "scavenge-id"
    val LeaderEndpointHost: String = "leader-endpoint-host"
    val LeaderEndpointPort: String = "leader-endpoint-port"

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
