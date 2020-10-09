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

import java.time.ZonedDateTime
import java.util.UUID
import scala.util.control.NoStackTrace
import cats.Show
import cats.syntax.all._
import scodec.bits.ByteVector
import sec.utilities.{guardNonEmpty, guardNotStartsWith}

//======================================================================================================================

sealed trait Event
object Event {

  implicit final class EventOps(val e: Event) extends AnyVal {

    def fold[A](f: EventRecord => A, g: ResolvedEvent => A): A = e match {
      case er: EventRecord   => f(er)
      case re: ResolvedEvent => g(re)
    }

    def streamId: StreamId        = e.fold(_.streamId, _.event.streamId)
    def number: EventNumber.Exact = e.fold(_.number, _.event.number)
    def position: Position.Exact  = e.fold(_.position, _.event.position)
    def eventData: EventData      = e.fold(_.eventData, _.event.eventData)
    def record: EventRecord       = e.fold(identity, _.link)
    def created: ZonedDateTime    = e.fold(_.created, _.event.created)
  }

  implicit val showForEvent: Show[Event] = Show.show[Event] {
    case er: EventRecord   => er.show
    case re: ResolvedEvent => re.show
  }

}

final case class EventRecord(
  streamId: StreamId,
  number: EventNumber.Exact,
  position: Position.Exact,
  eventData: EventData,
  created: ZonedDateTime
) extends Event

object EventRecord {

  implicit val showForEventRecord: Show[EventRecord] = Show.show[EventRecord] { er =>
    s"""
       |EventRecord(
       |  streamId = ${er.streamId.show},
       |  eventId  = ${er.eventData.eventId},
       |  type     = ${er.eventData.eventType.show},
       |  number   = ${er.number.show},
       |  position = ${er.position.show},
       |  data     = ${er.eventData.showData}, 
       |  metadata = ${er.eventData.showMetadata}, 
       |  created  = ${er.created}
       |)
       |""".stripMargin
  }

}

final case class ResolvedEvent(
  event: EventRecord,
  link: EventRecord
) extends Event

object ResolvedEvent {

  implicit val showForResolvedEvent: Show[ResolvedEvent] = Show.show[ResolvedEvent] { re =>
    s"""
       |ResolvedEvent(
       |  event = ${re.event.show},
       |  link  = ${re.link.show}
       |)
       |""".stripMargin
  }

}

//======================================================================================================================

sealed trait EventType
object EventType {

  sealed trait SystemType     extends EventType
  case object StreamDeleted   extends SystemType
  case object StatsCollected  extends SystemType
  case object LinkTo          extends SystemType
  case object StreamReference extends SystemType
  case object StreamMetadata  extends SystemType
  case object Settings        extends SystemType

  sealed abstract case class SystemDefined(name: String) extends SystemType
  sealed abstract case class UserDefined(name: String)   extends EventType

  def apply(name: String): Attempt[UserDefined]      = userDefined(name)
  def of[F[_]: ErrorA](name: String): F[UserDefined] = EventType(name).leftMap(InvalidEventType).liftTo[F]

  ///

  private[sec] val guardNonEmptyName: String => Attempt[String] = guardNonEmpty("Event type name")

  private[sec] def systemDefined(name: String): Attempt[SystemDefined] =
    guardNonEmptyName(name) >>= guardNotStartsWith(systemPrefix) >>= (n => new SystemDefined(n) {}.asRight)

  private[sec] def userDefined(name: String): Attempt[UserDefined] =
    guardNonEmptyName(name) >>= guardNotStartsWith(systemPrefix) >>= (n => new UserDefined(n) {}.asRight)

  ///

  private[sec] val eventTypeToString: EventType => String = {
    case StreamDeleted    => systemTypes.StreamDeleted
    case StatsCollected   => systemTypes.StatsCollected
    case LinkTo           => systemTypes.LinkTo
    case StreamReference  => systemTypes.StreamReference
    case StreamMetadata   => systemTypes.StreamMetadata
    case Settings         => systemTypes.Settings
    case SystemDefined(n) => s"$systemPrefix$n"
    case UserDefined(n)   => n
  }

  private[sec] val stringToEventType: String => Attempt[EventType] = {
    case systemTypes.StreamDeleted         => StreamDeleted.asRight
    case systemTypes.StatsCollected        => StatsCollected.asRight
    case systemTypes.LinkTo                => LinkTo.asRight
    case systemTypes.StreamReference       => StreamReference.asRight
    case systemTypes.StreamMetadata        => StreamMetadata.asRight
    case systemTypes.Settings              => Settings.asRight
    case sd if sd.startsWith(systemPrefix) => systemDefined(sd.substring(systemPrefixLength))
    case ud                                => userDefined(ud)
  }

  final private[sec] val systemPrefix: String    = "$"
  final private[sec] val systemPrefixLength: Int = systemPrefix.length

  private[sec] object systemTypes {
    final val StreamDeleted: String   = "$streamDeleted"
    final val StatsCollected: String  = "$statsCollected"
    final val LinkTo: String          = "$>"
    final val StreamReference: String = "$@"
    final val StreamMetadata: String  = "$metadata"
    final val Settings: String        = "$settings"
  }

  ///

  implicit val showForEventType: Show[EventType] = Show.show[EventType](eventTypeToString)

  final case class InvalidEventType(msg: String) extends RuntimeException(msg) with NoStackTrace

}

//======================================================================================================================

final case class EventData(
  eventType: EventType,
  eventId: UUID,
  data: ByteVector,
  metadata: ByteVector,
  contentType: ContentType
)

object EventData {

  def apply(
    eventType: EventType,
    eventId: UUID,
    data: ByteVector,
    contentType: ContentType
  ): EventData =
    EventData(eventType, eventId, data, ByteVector.empty, contentType)

  def apply(
    eventType: String,
    eventId: UUID,
    data: ByteVector,
    contentType: ContentType
  ): Attempt[EventData] =
    EventType(eventType).map(EventData(_, eventId, data, ByteVector.empty, contentType))

  def apply(
    eventType: String,
    eventId: UUID,
    data: ByteVector,
    metadata: ByteVector,
    contentType: ContentType
  ): Attempt[EventData] =
    EventType(eventType).map(EventData(_, eventId, data, metadata, contentType))

  def of[F[_]: ErrorA](
    eventType: String,
    eventId: UUID,
    data: ByteVector,
    contentType: ContentType
  ): F[EventData] =
    EventType.of[F](eventType).map(EventData(_, eventId, data, ByteVector.empty, contentType))

  def of[F[_]: ErrorA](
    eventType: String,
    eventId: UUID,
    data: ByteVector,
    metadata: ByteVector,
    contentType: ContentType
  ): F[EventData] =
    EventType.of[F](eventType).map(EventData(_, eventId, data, metadata, contentType))

  ///

  implicit final private[sec] class EventDataOps(val ed: EventData) extends AnyVal {
    def showData: String     = render(ed.data, ed.contentType)
    def showMetadata: String = render(ed.metadata, ed.contentType)
  }

  private[sec] def render(bv: ByteVector, ct: ContentType): String =
    if (bv.isEmpty || ct.isBinary) s"${ct.show}(${renderByteVector(bv)})"
    else s"${bv.decodeUtf8.getOrElse("Failed decoding utf8")}"

  private[sec] def renderByteVector(bv: ByteVector): String = {
    if (bv.isEmpty) s"empty"
    else if (bv.size < 32) s"${bv.size} bytes, 0x${bv.toHex}"
    else s"${bv.size} bytes, #${bv.hashCode}"
  }

}

//======================================================================================================================

sealed trait ContentType
object ContentType {

  case object Binary extends ContentType
  case object Json   extends ContentType

  implicit final class ContentTypeOps(val ct: ContentType) extends AnyVal {

    private[sec] def fold[A](binary: => A, json: => A): A = ct match {
      case Binary => binary
      case Json   => json
    }

    def isJson: Boolean   = ct.fold(false, true)
    def isBinary: Boolean = !isJson
  }

  implicit val showForType: Show[ContentType] = Show.show[ContentType] {
    case Binary => "Binary"
    case Json   => "Json"
  }

}

//======================================================================================================================
