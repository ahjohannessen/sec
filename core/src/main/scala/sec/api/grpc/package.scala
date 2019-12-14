package sec
package api

import cats.implicits._
import io.grpc.{Metadata, StatusRuntimeException}
import sec.core._
import grpc.constants.Headers._
import grpc.constants.{Exceptions => ce}

package object grpc {

//======================================================================================================================

  private[grpc] object keys {
    val authKey: Metadata.Key[UserCredentials] = Metadata.Key.of(Authorization, UserCredentialsMarshaller)
    val cnKey: Metadata.Key[String]            = Metadata.Key.of(ConnectionName, StringMarshaller)
    val exception: Metadata.Key[String]        = Metadata.Key.of(ce.ExceptionKey, StringMarshaller)
    val streamName: Metadata.Key[String]       = Metadata.Key.of(ce.StreamName, StringMarshaller)
    val groupName: Metadata.Key[String]        = Metadata.Key.of(ce.GroupName, StringMarshaller)
    val loginName: Metadata.Key[String]        = Metadata.Key.of(ce.LoginName, StringMarshaller)
    val expectedVersion: Metadata.Key[Long]    = Metadata.Key.of(ce.ExpectedVersion, LongMarshaller)
    val actualVersion: Metadata.Key[Long]      = Metadata.Key.of(ce.ActualVersion, LongMarshaller)
    val maximumAppendSize: Metadata.Key[Int]   = Metadata.Key.of(ce.MaximumAppendSize, IntMarshaller)
  }

//======================================================================================================================

  implicit final class ContextOps(val ctx: Context) extends AnyVal {
    def toMetadata: Metadata = {
      val md = new Metadata()
      ctx.userCreds.foreach(md.put(keys.authKey, _))
      md.put(keys.cnKey, ctx.connectionName)
      md
    }
  }

//======================================================================================================================

  val convertToEs: StatusRuntimeException => Option[EsException] = ex => {

    val unknown           = "<unknown>"
    val md                = ex.getTrailers
    val exception         = md.getOpt(keys.exception)
    def streamName        = md.getOpt(keys.streamName).getOrElse(unknown)
    def groupName         = md.getOpt(keys.groupName).getOrElse(unknown)
    def expected          = md.getOpt(keys.expectedVersion)
    def actual            = md.getOpt(keys.actualVersion)
    def loginName         = md.getOpt(keys.loginName).getOrElse(unknown)
    def maximumAppendSize = md.getOpt(keys.maximumAppendSize)

    exception.map {
      case ce.AccessDenied                       => AccessDenied
      case ce.InvalidTransaction                 => InvalidTransaction
      case ce.MaximumAppendSizeExceeded          => MaximumAppendSizeExceeded(maximumAppendSize)
      case ce.StreamDeleted                      => StreamDeleted(streamName)
      case ce.WrongExpectedVersion               => WrongExpectedVersion(streamName, expected, actual)
      case ce.StreamNotFound                     => StreamNotFound(streamName)
      case ce.PersistentSubscriptionDoesNotExist => PersistentSubscriptionNotFound(streamName, groupName)
      case ce.MaximumSubscribersReached          => PersistentSubscriptionMaximumSubscribersReached(streamName, groupName)
      case ce.PersistentSubscriptionDropped      => PersistentSubscriptionDroppedByServer(streamName, groupName)
      case ce.UserNotFound                       => UserNotFound(loginName)
    }
  }

//======================================================================================================================

  implicit final class MetadataOps(val md: Metadata) extends AnyVal {
    def getOpt[T](key: Metadata.Key[T]): Option[T] = Either.catchNonFatal(Option(md.get(key))).toOption.flatten
  }

//======================================================================================================================

}
