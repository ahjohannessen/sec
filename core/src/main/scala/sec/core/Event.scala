package sec
package core

import java.util.UUID
import java.time.ZonedDateTime
import cats.implicits._
import cats.Show
import constants.SystemEventTypes.LinkTo

sealed trait Event {
  def streamId: String // EventStream.Id
  def number: EventNumber.Exact
  def position: Position.Exact
  def eventData: EventData
  def record: EventRecord
  def created: ZonedDateTime
}

object Event {

  implicit final class EventOps(val e: Event) extends AnyVal {
    def link(eventId: UUID, metadata: Content = Content.Empty): Attempt[EventData] =
      Content(s"${e.number.value}@${e.streamId}") >>= (EventData(eventType = LinkTo, eventId, _, metadata))
  }

  implicit val showForEventRecord: Show[Event] = Show.show[Event] {
    case e: EventRecord   => e.show
    case r: ResolvedEvent => s"ResolvedEvent(linkedEvent = ${r.linkedEvent.show}, linkEvent = ${r.linkEvent.show})"
  }
}

final case class EventRecord(
  streamId: String, // EventStream.Id,
  number: EventNumber.Exact,
  position: Position.Exact,
  eventData: EventData,
  created: ZonedDateTime
) extends Event {
  def record = this
}

object EventRecord {

  // Temporay crap
  implicit val showForEventRecord: Show[EventRecord] = Show.show[EventRecord] { er =>
    import er._

    val dataShow = eventData.data.show
    val metaShow = if (eventData.metadata.bytes.nonEmpty) eventData.metadata.show else "n/a"

    f"EventRecord(id=$streamId, nr=${number.value}%03d, pos = (${position.commit}, ${position.prepare}), data = $dataShow, meta = $metaShow, created = ${created})"
  }

}

final case class ResolvedEvent(
  linkedEvent: EventRecord,
  linkEvent: EventRecord
) extends Event {
  def streamId  = linkedEvent.streamId
  def number    = linkedEvent.number
  def position  = linkedEvent.position
  def eventData = linkedEvent.eventData
  def record    = linkEvent
  def created   = linkedEvent.created
}
