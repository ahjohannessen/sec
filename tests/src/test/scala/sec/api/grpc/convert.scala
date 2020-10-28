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
package grpc

import cats.syntax.all._
import io.grpc.{Metadata, Status, StatusRuntimeException}
import org.specs2._
import sec.api.exceptions._
import sec.api.grpc.constants.{Exceptions => ce}
import sec.api.grpc.convert.{convertToEs, keys => k}

class ConvertSpec extends mutable.Specification {

  "convertToEs" >> {

    val ek       = k.exception
    val streamId = "streamId"
    val groupId  = "groupId"
    val reason   = "reason"
    val user     = "chuck norris"
    val unknown  = "<unknown>"

    val convert: (Metadata => Unit) => Option[EsException] = f => {

      val fn: Metadata => StatusRuntimeException =
        md => Status.INVALID_ARGUMENT.asRuntimeException(md)

      val meta: (Metadata => Unit) => Metadata = f => {
        val meta = new Metadata
        f(meta)
        meta
      }

      convertToEs(fn(meta(f)))
    }

    convert { m =>
      m.put(ek, ce.AccessDenied)
    } shouldEqual AccessDenied.some

    convert { m =>
      m.put(ek, ce.InvalidTransaction)
    } shouldEqual InvalidTransaction.some

    convert { m =>
      m.put(ek, ce.MaximumAppendSizeExceeded)
      m.put(k.maximumAppendSize, 4096)
    } should beSome(MaximumAppendSizeExceeded(4096.some))

    convert { m =>
      m.put(ek, ce.MaximumAppendSizeExceeded)
      m.put(Metadata.Key.of(ce.MaximumAppendSize, StringMarshaller), "a")
    } should beSome(MaximumAppendSizeExceeded(None))

    convert { m =>
      m.put(ek, ce.StreamDeleted)
      m.put(k.streamName, streamId)
    } should beSome(StreamDeleted(streamId))

    convert { m =>
      m.put(ek, ce.StreamDeleted)
    } should beSome(StreamDeleted(unknown))

    convert { m =>
      m.put(ek, ce.WrongExpectedVersion)
      m.put(k.streamName, streamId)
      m.put(k.actualVersion, 1L)
      m.put(k.expectedVersion, 2L)
    } should beSome(WrongExpectedVersion(streamId, 2L.some, 1L.some))

    convert { m =>
      m.put(ek, ce.WrongExpectedVersion)
      m.put(k.streamName, streamId)
      m.put(Metadata.Key.of(ce.ActualVersion, StringMarshaller), "a")
      m.put(Metadata.Key.of(ce.ExpectedVersion, StringMarshaller), "b")
    } should beSome(WrongExpectedVersion(streamId, None, None))

    convert { m =>
      m.put(ek, ce.WrongExpectedVersion)
      m.put(k.actualVersion, 1L)
      m.put(k.expectedVersion, 2L)
    } should beSome(WrongExpectedVersion(unknown, 2L.some, 1L.some))

    convert { m =>
      m.put(ek, ce.NotLeader)
      m.put(k.leaderEndpointHost, "127.0.0.1")
      m.put(k.leaderEndpointPort, 2113)
    } should beSome(NotLeader("127.0.0.1".some, 2113.some))

    convert { m =>
      m.put(ek, ce.NotLeader)
      m.put(k.leaderEndpointHost, "127.0.0.1")
      m.put(Metadata.Key.of(ce.LeaderEndpointPort, StringMarshaller), "b")
    } should beSome(NotLeader("127.0.0.1".some, None))

    convert { m =>
      m.put(ek, ce.StreamNotFound)
      m.put(k.streamName, streamId)
    } should beSome(StreamNotFound(streamId))

    convert { m =>
      m.put(ek, ce.StreamNotFound)
    } should beSome(StreamNotFound(unknown))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionFailed)
      m.put(k.streamName, streamId)
      m.put(k.groupName, groupId)
      m.put(k.reason, reason)
    } should beSome(PersistentSubscription.Failed(streamId, groupId, reason))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionFailed)
    } should beSome(PersistentSubscription.Failed(unknown, unknown, unknown))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionDoesNotExist)
      m.put(k.streamName, streamId)
      m.put(k.groupName, groupId)
    } should beSome(PersistentSubscription.NotFound(streamId, groupId))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionDoesNotExist)
    } should beSome(PersistentSubscription.NotFound(unknown, unknown))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionExists)
      m.put(k.streamName, streamId)
      m.put(k.groupName, groupId)
    } should beSome(PersistentSubscription.Exists(streamId, groupId))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionExists)
    } should beSome(PersistentSubscription.Exists(unknown, unknown))

    convert { m =>
      m.put(ek, ce.MaximumSubscribersReached)
      m.put(k.streamName, streamId)
      m.put(k.groupName, groupId)
    } should beSome(PersistentSubscription.MaximumSubscribersReached(streamId, groupId))

    convert { m =>
      m.put(ek, ce.MaximumSubscribersReached)
    } should beSome(PersistentSubscription.MaximumSubscribersReached(unknown, unknown))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionDropped)
      m.put(k.streamName, streamId)
      m.put(k.groupName, groupId)
    } should beSome(PersistentSubscription.Dropped(streamId, groupId))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionDropped)
    } should beSome(PersistentSubscription.Dropped(unknown, unknown))

    convert { m =>
      m.put(ek, ce.UserNotFound)
      m.put(k.loginName, user)
    } should beSome(UserNotFound(user))

    convert { m =>
      m.put(ek, ce.UserNotFound)
    } should beSome(UserNotFound(unknown))

    /// Unknown Exception Key

    convert { m =>
      m.put(ek, "not-handled")
    } should beSome(UnknownError("Exception key: not-handled"))

    /// From Status Codes & Causes

    convertToEs(Status.UNAVAILABLE.withDescription("Oops").asRuntimeException()) should beSome(
      ServerUnavailable("UNAVAILABLE: Oops")
    )

  }
}
