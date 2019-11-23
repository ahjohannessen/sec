package sec
package core

//import java.util.UUID
import java.time.ZonedDateTime
import cats.implicits._
import cats.Show

sealed trait Event extends Ordered[Event] {

  def streamId: String // EventStream.Id
  def number: EventNumber.Exact
  def position: Position.Exact
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

object Event {

  implicit val showForEventRecord: Show[Event] = Show.show[Event] {
    case e: EventRecord   => e.show
    case r: ResolvedEvent => s"ResolvedEvent(linkedEvent = ${r.linkedEvent.show}, linkEvent = ${r.linkEvent.show})"
  }

}

final case class EventRecord(
  streamId: String, // EventStream.Id,
  number: EventNumber.Exact,
  position: Position.Exact,
  data: EventData,
  created: ZonedDateTime
) extends Event {
  def record = this
}

object EventRecord {

  // Temporay crap
  implicit val showForEventRecord: Show[EventRecord] = Show.show[EventRecord] { er =>
    import er._

    val dataShow = data.data.show
    val metaShow = if (data.metadata.data.nonEmpty) data.metadata.show else "n/a"

    s"EventRecord(id=$streamId, nr=${number.value}, pos = (${position.commit}, ${position.prepare}), data = $dataShow, meta = $metaShow, created = ${created})"
  }

}

final case class ResolvedEvent(
  linkedEvent: EventRecord,
  linkEvent: EventRecord
) extends Event {
  def streamId = linkedEvent.streamId
  def number   = linkedEvent.number
  def position = linkedEvent.position
  def data     = linkedEvent.data
  def record   = linkEvent
  def created  = linkedEvent.created
}
