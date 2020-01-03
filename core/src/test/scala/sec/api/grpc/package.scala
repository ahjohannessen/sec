package sec
package api
package grpc

import io.grpc.{Metadata, Status, StatusRuntimeException}
import org.specs2._
import cats.implicits._
import sec.core._
import constants.{Exceptions => ce}
import grpc.{keys => k}

class GrpcPackageSpec extends mutable.Specification {

  "convertToEs" >> {

    val ek       = k.exception
    val streamId = "streamId"
    val groupId  = "groupId"
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
      m.put(ek, ce.StreamNotFound)
      m.put(k.streamName, streamId)
    } should beSome(StreamNotFound(streamId))

    convert { m =>
      m.put(ek, ce.StreamNotFound)
    } should beSome(StreamNotFound(unknown))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionDoesNotExist)
      m.put(k.streamName, streamId)
      m.put(k.groupName, groupId)
    } should beSome(PersistentSubscriptionNotFound(streamId, groupId))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionDoesNotExist)
    } should beSome(PersistentSubscriptionNotFound(unknown, unknown))

    convert { m =>
      m.put(ek, ce.MaximumSubscribersReached)
      m.put(k.streamName, streamId)
      m.put(k.groupName, groupId)
    } should beSome(PersistentSubscriptionMaximumSubscribersReached(streamId, groupId))

    convert { m =>
      m.put(ek, ce.MaximumSubscribersReached)
    } should beSome(PersistentSubscriptionMaximumSubscribersReached(unknown, unknown))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionDropped)
      m.put(k.streamName, streamId)
      m.put(k.groupName, groupId)
    } should beSome(PersistentSubscriptionDroppedByServer(streamId, groupId))

    convert { m =>
      m.put(ek, ce.PersistentSubscriptionDropped)
    } should beSome(PersistentSubscriptionDroppedByServer(unknown, unknown))

    convert { m =>
      m.put(ek, ce.UserNotFound)
      m.put(k.loginName, user)
    } should beSome(UserNotFound(user))

    convert { m =>
      m.put(ek, ce.UserNotFound)
    } should beSome(UserNotFound(unknown))

  }
}
