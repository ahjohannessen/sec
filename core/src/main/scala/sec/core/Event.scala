package sec
package core

//import java.util.UUID
import java.time.ZonedDateTime
//import cats.implicits._

sealed trait Event extends Ordered[Event] {

  def streamId: String // EventStream.Id
  def number: EventNumber.Exact
  def data: EventData
  def record: EventRecord
  def created: ZonedDateTime

  final def compare(that: Event) = number.value compare that.number.value

//  /// TODO: Circle back to this
//  final def link(eventId: UUID, metadata: Content = Content.Empty): Attempt[EventData] = {
//    Content(s"${number.value}@${streamId}") >>= { c =>
//      EventData(eventType = "$>", eventId, c, metadata)
//    }
//  }
}

final case class EventRecord(
  streamId: String, // EventStream.Id,
  number: EventNumber.Exact,
  data: EventData,
  created: ZonedDateTime
) extends Event {
  def record = this
}

final case class ResolvedEvent(
  linkedEvent: EventRecord,
  linkEvent: EventRecord
) extends Event {
  def streamId = linkedEvent.streamId
  def number   = linkedEvent.number
  def data     = linkedEvent.data
  def record   = linkEvent
  def created  = linkedEvent.created
}
