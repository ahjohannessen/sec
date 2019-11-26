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

    def streamId: String          = e.fold(_.streamId, _.event.streamId)
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
  streamId: String, // Strong types: User & SystemStreams,
  number: EventNumber.Exact,
  position: Position.Exact,
  eventData: EventData,
  created: ZonedDateTime
) extends Event

object EventRecord {

  implicit final class EventRecordOps(val e: EventRecord) extends AnyVal {

    import constants.SystemEventTypes.LinkTo

    def createLink(eventId: UUID): Attempt[EventData] =
      Either.cond(e.eventData.eventType != LinkTo, (), "Linking to a link is not supported.") >> {
        Content.binary(s"${e.number.value}@${e.streamId}") >>= (EventData(eventType = LinkTo, eventId, _))
      }
  }

  ///

  implicit val showForEventRecord: Show[EventRecord] = Show.show[EventRecord] { er =>
    s"""
       |EventRecord(
       |  streamId = ${er.streamId}, 
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

sealed abstract case class EventData(
  eventType: String, // Strong types: User & SystemEventTypes
  eventId: UUID,
  data: Content,
  metadata: Content
)

object EventData {

  def apply(eventType: String, eventId: UUID, data: Content): Attempt[EventData] =
    EventData(eventType, eventId, data, Content(ByteVector.empty, data.contentType))

  def apply(eventType: String, eventId: UUID, data: Content, metadata: Content): Attempt[EventData] = {

    val guardEventType = Either.fromOption(
      Option(eventType).filter(_.nonEmpty),
      "eventType cannot be empty"
    )

    val guardContentType = Either.cond(
      data.contentType == metadata.contentType,
      (data, metadata),
      "ES does not support different content types for data & metadata."
    )

    (guardContentType, guardEventType).mapN {
      case ((gdata, gmeta), gtype) => new EventData(gtype, eventId, gdata, gmeta) {}
    }

  }

  def json(eventType: String, eventId: UUID, data: ByteVector, metadata: ByteVector): Attempt[EventData] =
    EventData(eventType, eventId, Content(data, Content.Type.Json), Content(metadata, Content.Type.Json))

  def binary(eventType: String, eventId: UUID, data: ByteVector, metadata: ByteVector): Attempt[EventData] =
    EventData(eventType, eventId, Content(data, Content.Type.Binary), Content(metadata, Content.Type.Binary))

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

  val Empty: Content = Content(ByteVector.empty, Type.Binary)

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
