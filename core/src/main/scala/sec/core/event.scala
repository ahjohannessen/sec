package sec
package core

import java.util.UUID
import java.time.ZonedDateTime
import cats.Show
import cats.implicits._
import scodec.bits.ByteVector

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

  implicit final class EventRecordOps(val e: EventRecord) extends AnyVal {

    def createLink(eventId: UUID): Attempt[EventData] =
      Either.cond(e.eventData.eventType != EventType.LinkTo, (), "Linking to a link is not supported.") >> {
        Content.binary(s"${e.number.value}@${e.streamId.stringValue}") >>= { data =>
          EventData(EventType.LinkTo, eventId, data, Content.BinaryEmpty)
        }
      }
  }

  ///

  implicit val showForEventRecord: Show[EventRecord] = Show.show[EventRecord] { er =>
    s"""
       |EventRecord(
       |  streamId = ${er.streamId.show}, 
       |  number   = ${er.number.show},
       |  position = ${er.position.show},
       |  data     = ${er.eventData.data.show}, 
       |  metadata = ${er.eventData.metadata.show}, 
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

  def apply(name: String): Attempt[UserDefined] = userDefined(name)

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

  private[sec] final val systemPrefix: String    = "$"
  private[sec] final val systemPrefixLength: Int = systemPrefix.length

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

}

//======================================================================================================================

sealed abstract case class EventData(
  eventType: EventType,
  eventId: UUID,
  data: Content,
  metadata: Content
)

object EventData {

  def apply(eventType: String, eventId: UUID, data: Content): Attempt[EventData] =
    EventData(eventType, eventId, data, Content(ByteVector.empty, data.contentType))

  def apply(eventType: String, eventId: UUID, data: Content, metadata: Content): Attempt[EventData] =
    EventType(eventType) >>= (EventData(_, eventId, data, metadata))

  private[sec] def apply(et: EventType, eventId: UUID, data: Content): Attempt[EventData] =
    EventData(et, eventId, data, Content(ByteVector.empty, data.contentType))

  private[sec] def apply(et: EventType, eventId: UUID, data: Content, metadata: Content): Attempt[EventData] =
    if (data.contentType == metadata.contentType) new EventData(et, eventId, data, metadata) {}.asRight
    else "Different content types for data & metadata is not supported.".asLeft

  private[sec] def json(et: EventType, eventId: UUID, data: ByteVector, metadata: ByteVector): Attempt[EventData] =
    EventData(et, eventId, Content(data, Content.Type.Json), Content(metadata, Content.Type.Json))

  private[sec] def binary(et: EventType, eventId: UUID, data: ByteVector, metadata: ByteVector): Attempt[EventData] =
    EventData(et, eventId, Content(data, Content.Type.Binary), Content(metadata, Content.Type.Binary))

  ///

  implicit class EventDataOps(ed: EventData) {
    def isJson: Boolean = ed.data.contentType.isJson
  }

}

//======================================================================================================================

final case class Content(
  bytes: ByteVector,
  contentType: Content.Type
)

object Content {

  sealed trait Type
  object Type {

    case object Binary extends Type
    case object Json   extends Type

    final implicit class TypeOps(val tpe: Type) extends AnyVal {

      def fold[A](binary: => A, json: => A): A = tpe match {
        case Binary => binary
        case Json   => json
      }

      def isJson: Boolean   = tpe.fold(false, true)
      def isBinary: Boolean = tpe.fold(true, false)
    }

    implicit val showForType: Show[Type] = Show.show[Type] {
      case Binary => "Binary"
      case Json   => "Json"
    }

  }

  ///

  def empty(t: Type): Content = Content(ByteVector.empty, t)
  val BinaryEmpty: Content    = empty(Type.Binary)
  val JsonEmpty: Content      = empty(Type.Json)

  def apply(data: String, ct: Type): Attempt[Content] =
    ByteVector.encodeUtf8(data).map(Content(_, ct)).leftMap(_.getMessage)

  def binary(data: String): Attempt[Content] = Content(data, Type.Binary)
  def json(data: String): Attempt[Content]   = Content(data, Type.Json)

  ///

  implicit val showByteVector: Show[ByteVector] = Show.show[ByteVector] { bv =>
    if (bv.isEmpty) s"empty"
    else if (bv.size < 32) s"${bv.size} bytes, 0x${bv.toHex}"
    else s"${bv.size} bytes, #${bv.hashCode}"
  }

  implicit val showForContent: Show[Content] = Show.show {
    case Content(b, t) if b.isEmpty || t.isBinary => s"${t.show}(${showByteVector.show(b)})"
    case Content(b, t) if t.isJson                => s"${b.decodeUtf8.getOrElse("Failed decoding utf8")}"
  }

}

//======================================================================================================================
