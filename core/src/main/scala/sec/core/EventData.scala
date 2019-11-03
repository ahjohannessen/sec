package sec
package core

import java.util.UUID
import cats.implicits._

sealed abstract case class EventData(
  eventType: String,
  eventId: UUID,
  data: Content,
  metadata: Content
)

object EventData {

  def apply(eventType: String, eventId: UUID): Attempt[EventData] =
    EventData(eventType, eventId, Content.Empty)

  def apply(eventType: String, eventId: UUID, data: Content): Attempt[EventData] =
    EventData(eventType, eventId, data, Content.Empty)

  def apply(eventType: String, eventId: UUID, data: Content, metadata: Content): Attempt[EventData] =
    if (Option(eventType).filter(_.isEmpty).isDefined) "eventType cannot be empty or null".asLeft
    else new EventData(eventType, eventId, data, metadata) {}.asRight

  ///

  implicit class EventDataOps(ed: EventData) {
    def isJson: Boolean = ed.data.ct.isJson
  }

}
