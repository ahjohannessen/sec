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
package grpc

import cats.syntax.all.*
import io.grpc.{Metadata, Status, StatusRuntimeException}
import sec.api.exceptions.*
import sec.api.grpc.constants.Exceptions as ce
import sec.api.grpc.convert.{convertToEs, keys as k}

class ConvertSuite extends SecSuite:

  test("convertToEs") {

    val ek       = k.exception
    val streamId = "streamId"
    val groupId  = "groupId"
    val reason   = "reason"
    val user     = "chuck norris"
    val unknown  = "<unknown>"

    val convert: (Metadata => Unit) => Option[EsException] = f =>

      val fn: Metadata => StatusRuntimeException =
        md => Status.INVALID_ARGUMENT.asRuntimeException(md)

      val meta: (Metadata => Unit) => Metadata = f => {
        val meta = new Metadata
        f(meta)
        meta
      }

      convertToEs(fn(meta(f)))

    assertEquals(
      convert(_.put(ek, ce.AccessDenied)),
      AccessDenied.some
    )

    assertEquals(
      convert(_.put(ek, ce.InvalidTransaction)),
      InvalidTransaction.some
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.MaximumAppendSizeExceeded)
        m.put(k.maximumAppendSize, 4096)
      },
      Some(MaximumAppendSizeExceeded(4096.some))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.MaximumAppendSizeExceeded)
        m.put(Metadata.Key.of(ce.MaximumAppendSize, StringMarshaller), "a")
      },
      Some(MaximumAppendSizeExceeded(None))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.StreamDeleted)
        m.put(k.streamName, streamId)
      },
      Some(StreamDeleted(streamId))
    )

    assertEquals(
      convert(_.put(ek, ce.StreamDeleted)),
      Some(StreamDeleted(unknown))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.WrongExpectedVersion)
        m.put(k.streamName, streamId)
        m.put(k.actualVersion, 1L)
        m.put(k.expectedVersion, 2L)
      },
      Some(WrongExpectedVersion(streamId, 2L.some, 1L.some))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.WrongExpectedVersion)
        m.put(k.streamName, streamId)
        m.put(Metadata.Key.of(ce.ActualVersion, StringMarshaller), "a")
        m.put(Metadata.Key.of(ce.ExpectedVersion, StringMarshaller), "b")
      },
      Some(WrongExpectedVersion(streamId, None, None))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.WrongExpectedVersion)
        m.put(k.actualVersion, 1L)
        m.put(k.expectedVersion, 2L)
      },
      Some(WrongExpectedVersion(unknown, 2L.some, 1L.some))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.NotLeader)
        m.put(k.leaderEndpointHost, "127.0.0.1")
        m.put(k.leaderEndpointPort, 2113)
      },
      Some(NotLeader("127.0.0.1".some, 2113.some))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.NotLeader)
        m.put(k.leaderEndpointHost, "127.0.0.1")
        m.put(Metadata.Key.of(ce.LeaderEndpointPort, StringMarshaller), "b")
      },
      Some(NotLeader("127.0.0.1".some, None))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.StreamNotFound)
        m.put(k.streamName, streamId)
      },
      Some(StreamNotFound(streamId))
    )

    assertEquals(
      convert(_.put(ek, ce.StreamNotFound)),
      Some(StreamNotFound(unknown))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.PersistentSubscriptionFailed)
        m.put(k.streamName, streamId)
        m.put(k.groupName, groupId)
        m.put(k.reason, reason)
      },
      Some(PersistentSubscription.Failed(streamId, groupId, reason))
    )

    assertEquals(
      convert(_.put(ek, ce.PersistentSubscriptionFailed)),
      Some(PersistentSubscription.Failed(unknown, unknown, unknown))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.PersistentSubscriptionDoesNotExist)
        m.put(k.streamName, streamId)
        m.put(k.groupName, groupId)
      },
      Some(PersistentSubscription.NotFound(streamId, groupId))
    )

    assertEquals(
      convert(_.put(ek, ce.PersistentSubscriptionDoesNotExist)),
      Some(PersistentSubscription.NotFound(unknown, unknown))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.PersistentSubscriptionExists)
        m.put(k.streamName, streamId)
        m.put(k.groupName, groupId)
      },
      Some(PersistentSubscription.Exists(streamId, groupId))
    )

    assertEquals(
      convert(_.put(ek, ce.PersistentSubscriptionExists)),
      Some(PersistentSubscription.Exists(unknown, unknown))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.MaximumSubscribersReached)
        m.put(k.streamName, streamId)
        m.put(k.groupName, groupId)
      },
      Some(PersistentSubscription.MaximumSubscribersReached(streamId, groupId))
    )

    assertEquals(
      convert(_.put(ek, ce.MaximumSubscribersReached)),
      Some(PersistentSubscription.MaximumSubscribersReached(unknown, unknown))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.PersistentSubscriptionDropped)
        m.put(k.streamName, streamId)
        m.put(k.groupName, groupId)
      },
      Some(PersistentSubscription.Dropped(streamId, groupId))
    )

    assertEquals(
      convert(_.put(ek, ce.PersistentSubscriptionDropped)),
      Some(PersistentSubscription.Dropped(unknown, unknown))
    )

    assertEquals(
      convert { m =>
        m.put(ek, ce.UserNotFound)
        m.put(k.loginName, user)
      },
      Some(UserNotFound(user))
    )

    assertEquals(
      convert(_.put(ek, ce.UserNotFound)),
      Some(UserNotFound(unknown))
    )

    // / Unknown Exception Key

    assertEquals(
      convert(_.put(ek, "not-handled")),
      Some(UnknownError("Exception key: not-handled"))
    )

    // / From Status Codes & Causes

    assertEquals(
      convertToEs(Status.UNAVAILABLE.withDescription("Oops").asRuntimeException()),
      Some(ServerUnavailable("UNAVAILABLE: Oops"))
    )

    assertEquals(
      convertToEs(
        Status.ABORTED
          .withDescription(
            "Operation timed out: Consumer too slow to handle event while live. Client resubscription required.")
          .asRuntimeException()
      ),
      Some(
        ResubscriptionRequired(
          "ABORTED: Operation timed out: Consumer too slow to handle event while live. Client resubscription required.")
      )
    )

  }
