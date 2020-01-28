package sec
package core

import cats.implicits._
import org.scalacheck._
import cats.kernel.laws.discipline._
import org.specs2.mutable.Specification
import org.typelevel.discipline.specs2.mutable.Discipline
import Arbitraries._

class StreamIdSpec extends Specification with Discipline {

  import StreamId.{systemStreams => ss}

  type ErrorOr[A] = Either[Throwable, A]

  val normalId = StreamId.normal("normal").unsafe
  val systemId = StreamId.system("system").unsafe

  "from" >> {
    StreamId.from("") should beLeft("name cannot be empty")
    StreamId.from("$$meta") should beLeft("value must not start with $$, but is $$meta")
    StreamId.from("$users") shouldEqual StreamId.system("users")
    StreamId.from("users") shouldEqual StreamId.normal("users")
    StreamId.from(ss.All) shouldEqual StreamId.All.asRight
    StreamId.from(ss.Settings) shouldEqual StreamId.Settings.asRight
    StreamId.from(ss.Stats) shouldEqual StreamId.Stats.asRight
    StreamId.from(ss.Scavenges) shouldEqual StreamId.Scavenges.asRight
    StreamId.from(ss.Streams) shouldEqual StreamId.Streams.asRight
  }

  "apply" >> {
    StreamId[ErrorOr]("") should beLeft(StreamId.StreamIdError("name cannot be empty"))
    StreamId[ErrorOr]("$$m") should beLeft(StreamId.StreamIdError("value must not start with $$, but is $$m"))
    StreamId[ErrorOr]("$users") shouldEqual StreamId.system("users")
    StreamId[ErrorOr]("users") shouldEqual StreamId.normal("users")
    StreamId[ErrorOr](ss.All) shouldEqual StreamId.All.asRight
    StreamId[ErrorOr](ss.Settings) shouldEqual StreamId.Settings.asRight
    StreamId[ErrorOr](ss.Stats) shouldEqual StreamId.Stats.asRight
    StreamId[ErrorOr](ss.Scavenges) shouldEqual StreamId.Scavenges.asRight
    StreamId[ErrorOr](ss.Streams) shouldEqual StreamId.Streams.asRight
  }

  "streamIdToString" >> {
    StreamId.streamIdToString(StreamId.All) shouldEqual ss.All
    StreamId.streamIdToString(StreamId.Settings) shouldEqual ss.Settings
    StreamId.streamIdToString(StreamId.Stats) shouldEqual ss.Stats
    StreamId.streamIdToString(StreamId.Scavenges) shouldEqual ss.Scavenges
    StreamId.streamIdToString(StreamId.Streams) shouldEqual ss.Streams
    StreamId.streamIdToString(systemId) shouldEqual "$system"
    StreamId.streamIdToString(normalId) shouldEqual "normal"
    StreamId.streamIdToString(systemId.metaId) shouldEqual "$$$system"
    StreamId.streamIdToString(normalId.metaId) shouldEqual "$$normal"
  }

  "stringToStreamId" >> {
    StreamId.stringToStreamId("$$normal") shouldEqual normalId.metaId.asRight
    StreamId.stringToStreamId("$$$system") shouldEqual systemId.metaId.asRight
    StreamId.stringToStreamId(ss.All) shouldEqual StreamId.All.asRight
    StreamId.stringToStreamId(ss.Settings) shouldEqual StreamId.Settings.asRight
    StreamId.stringToStreamId(ss.Stats) shouldEqual StreamId.Stats.asRight
    StreamId.stringToStreamId(ss.Scavenges) shouldEqual StreamId.Scavenges.asRight
    StreamId.stringToStreamId(ss.Streams) shouldEqual StreamId.Streams.asRight
    StreamId.stringToStreamId(systemId.stringValue) should beRight(systemId)
    StreamId.stringToStreamId(normalId.stringValue) should beRight(normalId)
  }

  "show" >> {
    val sid = sampleOf[StreamId]
    sid.show shouldEqual sid.stringValue
  }

  "StreamIdOps" >> {
    "stringValue" >> {
      val sid = sampleOf[StreamId]
      sid.stringValue shouldEqual StreamId.streamIdToString(sid)
    }
  }

  "IdOps" >> {
    "meta" >> {
      val id = sampleOf[StreamId.Id]
      id.metaId shouldEqual StreamId.MetaId(id)
    }
  }

  "Eq" >> {
    implicit val cogen: Cogen[StreamId] = Cogen[String].contramap[StreamId](_.show)
    checkAll("StreamId", EqTests[StreamId].eqv)
  }

}
