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

import cats.syntax.all._
import io.grpc.{Metadata, Status, StatusRuntimeException}
import java.nio.channels.ClosedChannelException
import sec.api.grpc.constants.{Exceptions => ce}
import sec.api.exceptions._

private[sec] object convert {

//======================================================================================================================

  private[grpc] object keys {
    val exception: Metadata.Key[String]          = Metadata.Key.of(ce.ExceptionKey, StringMarshaller)
    val streamName: Metadata.Key[String]         = Metadata.Key.of(ce.StreamName, StringMarshaller)
    val groupName: Metadata.Key[String]          = Metadata.Key.of(ce.GroupName, StringMarshaller)
    val reason: Metadata.Key[String]             = Metadata.Key.of(ce.Reason, StringMarshaller)
    val loginName: Metadata.Key[String]          = Metadata.Key.of(ce.LoginName, StringMarshaller)
    val expectedVersion: Metadata.Key[Long]      = Metadata.Key.of(ce.ExpectedVersion, LongMarshaller)
    val actualVersion: Metadata.Key[Long]        = Metadata.Key.of(ce.ActualVersion, LongMarshaller)
    val maximumAppendSize: Metadata.Key[Int]     = Metadata.Key.of(ce.MaximumAppendSize, IntMarshaller)
    val leaderEndpointHost: Metadata.Key[String] = Metadata.Key.of(ce.LeaderEndpointHost, StringMarshaller)
    val leaderEndpointPort: Metadata.Key[Int]    = Metadata.Key.of(ce.LeaderEndpointPort, IntMarshaller)
  }

//======================================================================================================================

  implicit final private[grpc] class MetadataOps(val md: Metadata) extends AnyVal {
    def getOpt[T](key: Metadata.Key[T]): Option[T] = Either.catchNonFatal(Option(md.get(key))).toOption.flatten
  }

  val convertToEs: StatusRuntimeException => Option[EsException] = ex => {

    val unknown            = "<unknown>"
    val md                 = ex.getTrailers
    val exception          = md.getOpt(keys.exception)
    def streamName         = md.getOpt(keys.streamName).getOrElse(unknown)
    def groupName          = md.getOpt(keys.groupName).getOrElse(unknown)
    def reason             = md.getOpt(keys.reason).getOrElse(unknown)
    def expected           = md.getOpt(keys.expectedVersion)
    def actual             = md.getOpt(keys.actualVersion)
    def loginName          = md.getOpt(keys.loginName).getOrElse(unknown)
    def maximumAppendSize  = md.getOpt(keys.maximumAppendSize)
    def leaderEndpointHost = md.getOpt(keys.leaderEndpointHost)
    def leaderEndpointPort = md.getOpt(keys.leaderEndpointPort)

    val reified: Option[EsException] = exception.map {
      case ce.AccessDenied                       => AccessDenied
      case ce.InvalidTransaction                 => InvalidTransaction
      case ce.StreamDeleted                      => StreamDeleted(streamName)
      case ce.WrongExpectedVersion               => WrongExpectedVersion(streamName, expected, actual)
      case ce.StreamNotFound                     => StreamNotFound(streamName)
      case ce.MaximumAppendSizeExceeded          => MaximumAppendSizeExceeded(maximumAppendSize)
      case ce.NotLeader                          => NotLeader(leaderEndpointHost, leaderEndpointPort)
      case ce.PersistentSubscriptionFailed       => PersistentSubscription.Failed(streamName, groupName, reason)
      case ce.PersistentSubscriptionDoesNotExist => PersistentSubscription.NotFound(streamName, groupName)
      case ce.PersistentSubscriptionExists       => PersistentSubscription.Exists(streamName, groupName)
      case ce.MaximumSubscribersReached          => PersistentSubscription.MaximumSubscribersReached(streamName, groupName)
      case ce.PersistentSubscriptionDropped      => PersistentSubscription.Dropped(streamName, groupName)
      case ce.UserNotFound                       => UserNotFound(loginName)
      case unknown                               => UnknownError(s"Exception key: $unknown")
    }

    reified orElse serverUnavailable(ex)
  }

  private val serverUnavailable: StatusRuntimeException => Option[ServerUnavailable] = { ex =>

    val status = ex.getStatus
    val code   = status.getCode
    val cause  = status.getCause

    val unavailable = code == Status.Code.UNAVAILABLE
    def chClosed    = code == Status.Code.UNKNOWN && cause.isInstanceOf[ClosedChannelException]

    def prefix: String =
      "Server Unavailable: "

    def noDescription: String =
      s"${prefix}No description specified."

    def channelClosed: String =
      s"${prefix}Channel closed."

    def normalizedMsg(msg: String): String =
      s"${prefix}${msg.replace("UNAVAILABLE:", "").replace("UNAVAILABLE", "").trim}"

    def exMsg: Option[String] =
      Option(ex.getMessage).map(normalizedMsg).filter(_.isEmpty)

    val msg = Option
      .when(unavailable)(exMsg.getOrElse(noDescription))
      .getOrElse(channelClosed)

    Option.when(unavailable || chClosed)(ServerUnavailable(msg))
  }

//======================================================================================================================

}
