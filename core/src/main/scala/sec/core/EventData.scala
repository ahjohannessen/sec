package sec
package core

import java.util.UUID
import cats.implicits._
import scodec.bits.ByteVector

sealed abstract case class EventData(
  eventType: String,
  eventId: UUID,
  data: Content,
  metadata: Content
)

object EventData {

  def apply(eventType: String, eventId: UUID, data: Content): Attempt[EventData] =
    EventData(eventType, eventId, data, Content(ByteVector.empty, data.contentType)) // TODO: circle back. ES only supports content type to be same for data / metadata

  def apply(eventType: String, eventId: UUID, data: Content, metadata: Content): Attempt[EventData] =
    if (Option(eventType).filter(_.isEmpty).isDefined) "eventType cannot be empty or null".asLeft
    else new EventData(eventType, eventId, data, metadata) {}.asRight

  def json(eventType: String, eventId: UUID, data: ByteVector, metadata: ByteVector): Attempt[EventData] =
    EventData(eventType, eventId, Content(data, ContentType.Json), Content(metadata, ContentType.Json))

  def binary(eventType: String, eventId: UUID, data: ByteVector, metadata: ByteVector): Attempt[EventData] =
    EventData(eventType, eventId, Content(data, ContentType.Binary), Content(metadata, ContentType.Binary))

  ///

  implicit class EventDataOps(ed: EventData) {
    def isJson: Boolean = ed.data.contentType.isJson
  }

}
