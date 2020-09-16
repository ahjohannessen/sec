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

import java.time.{ZoneOffset, ZonedDateTime}
import java.{util => ju}
import scala.annotation.tailrec
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.collection.immutable.Nil
import cats.data.NonEmptyList
import cats.implicits._
import scodec.bits.ByteVector
import sec.core._
import sec.core.StreamRevision.{Any, NoStream, StreamExists}
import sec.api.Gossip._
import sec.api.Endpoint
import org.scalacheck._
import org.scalacheck.Arbitrary.arbitrary

object arbitraries {

  final def sampleOf[T](implicit ev: Arbitrary[T]): T =
    sampleOfGen(ev.arbitrary)

  @tailrec
  final def sampleOfGen[T](implicit g: Gen[T]): T = g.sample match {
    case Some(t) => t
    case None    => sampleOfGen[T](g)
  }

//======================================================================================================================
// Std Instances
//======================================================================================================================

  implicit val arbZonedDateTime: Arbitrary[ZonedDateTime] = Arbitrary(
    Gen.choose(-86400000L, 0L).map(ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(_))
  )

//======================================================================================================================
// EventNumber, Position & StreamRevision
//======================================================================================================================

  implicit val arbEventNumberExact: Arbitrary[EventNumber.Exact] = Arbitrary[EventNumber.Exact](
    Gen.chooseNum(0L, Long.MaxValue).map(EventNumber.Exact(_).leftMap(require(false, _)).toOption.get)
  )

  implicit val arbEventNumber: Arbitrary[EventNumber] =
    Arbitrary[EventNumber](Gen.oneOf(List(EventNumber.End, sampleOf[EventNumber.Exact])))

  implicit val arbPositionExact: Arbitrary[Position.Exact] = Arbitrary[Position.Exact](for {
    c <- Gen.chooseNum(0L, Long.MaxValue)
    p <- Gen.chooseNum(0L, 10L).map(c - _).suchThat(_ >= 0)
  } yield Position.Exact(c, p).leftMap(require(false, _)).toOption.get)

  implicit val arbPosition: Arbitrary[Position] =
    Arbitrary[Position](Gen.oneOf(List(Position.End, sampleOf[Position.Exact])))

  implicit val arbStreamRevision: Arbitrary[StreamRevision] =
    Arbitrary[StreamRevision](Gen.oneOf(List(NoStream, Any, StreamExists, sampleOf[EventNumber.Exact])))

//======================================================================================================================
// StreamId
//======================================================================================================================

  private[sec] object idGen {

    def genStreamIdNormal(prefix: String): Gen[StreamId.Normal] =
      Gen.identifier
        .suchThat(s => s.nonEmpty && s.length >= 3 && s.length <= 15 && !s.startsWith(StreamId.systemPrefix))
        .map(n => StreamId.normal(s"$prefix$n").unsafe)

  }

  implicit val arbStreamIdNormal: Arbitrary[StreamId.Normal] =
    Arbitrary[StreamId.Normal](idGen.genStreamIdNormal(""))

  implicit val arbStreamIdSystem: Arbitrary[StreamId.System] =
    Arbitrary[StreamId.System](StreamId.system(sampleOf[StreamId.Normal].name).unsafe)

  implicit val arbStreamIdNormalId: Arbitrary[StreamId.NormalId] =
    Arbitrary[StreamId.NormalId](sampleOf[StreamId.Normal])

  implicit val arbStreamIdSystemId: Arbitrary[StreamId.SystemId] = Arbitrary[StreamId.SystemId] {
    import StreamId._
    Gen.oneOf(All, Settings, Stats, Scavenges, Streams, sampleOf[System])
  }

  implicit val arbStreamIdId: Arbitrary[StreamId.Id] =
    Arbitrary[StreamId.Id](Gen.oneOf(sampleOf[StreamId.Normal], sampleOf[StreamId.SystemId]))

  implicit val arbStreamIdMetaId: Arbitrary[StreamId.MetaId] =
    Arbitrary[StreamId.MetaId](Gen.oneOf(sampleOf[StreamId.SystemId], sampleOf[StreamId.Normal]).map(_.metaId))

  implicit val arbStreamId: Arbitrary[StreamId] =
    Arbitrary[StreamId](Gen.oneOf(sampleOf[StreamId.Id], sampleOf[StreamId.MetaId]))

//======================================================================================================================
// EventType
//======================================================================================================================

  private[sec] object eventTypeGen {

    val defaultPrefix = "com.eventstore.client.Event"

    def genEventTypeUserDefined(prefix: String): Gen[EventType.UserDefined] = {

      val gen: Gen[String] = for {
        c  <- Gen.alphaUpperChar
        cs <- Gen.listOfN(2, Gen.alphaLowerChar)
      } yield s"$prefix${(c :: cs).mkString}"

      gen.map(et => EventType.userDefined(et).unsafe)
    }

  }

  implicit val arbEventTypeUserDefined: Arbitrary[EventType.UserDefined] = {
    import eventTypeGen._
    Arbitrary[EventType.UserDefined](genEventTypeUserDefined(defaultPrefix))
  }

  implicit val arbEventTypeSystemDefined: Arbitrary[EventType.SystemDefined] =
    Arbitrary[EventType.SystemDefined](EventType.systemDefined(sampleOf[EventType.UserDefined].name).unsafe)

  implicit val arbEventTypeSystemType: Arbitrary[EventType.SystemType] = Arbitrary[EventType.SystemType] {
    import EventType._
    Gen.oneOf(StreamDeleted, StatsCollected, LinkTo, StreamReference, StreamMetadata, Settings, sampleOf[SystemDefined])
  }

  implicit val arbEventType: Arbitrary[EventType] =
    Arbitrary[EventType](Gen.oneOf(sampleOf[EventType.SystemType], sampleOf[EventType.UserDefined]))

//======================================================================================================================
// Metadata
//======================================================================================================================

  implicit val arbStreamAcl: Arbitrary[StreamAcl] = Arbitrary[StreamAcl] {

    val roles: Set[String]                      = Set("role1", "role2", "role3", "role4", "role5")
    val someOf: Set[String] => Gen[Set[String]] = Gen.someOf(_).map(s => SortedSet(s.toSeq: _*))

    for {
      rr  <- someOf(roles).label("read")
      wr  <- someOf(rr).label("write")
      dr  <- someOf(wr).label("delete")
      mrr <- someOf(rr).label("meta-read")
      mwr <- someOf(dr).label("meta-write")
    } yield StreamAcl(rr, wr, dr, mrr, mwr)

  }

  implicit val arbStreamState: Arbitrary[StreamState] = Arbitrary[StreamState] {

    val oneYear = 31536000L
    val seconds = Gen.chooseNum(1L, oneYear).map(FiniteDuration(_, SECONDS))

    for {
      maxAge         <- Gen.option(seconds.map(MaxAge(_)))
      maxCount       <- Gen.option(Gen.chooseNum(1, Int.MaxValue).map(MaxCount(_)))
      truncateBefore <- Gen.option(arbEventNumberExact.arbitrary.suchThat(_ > EventNumber.Start))
      cacheControl   <- Gen.option(seconds.map(CacheControl(_)))
      acl            <- Gen.option(arbStreamAcl.arbitrary)
    } yield StreamState(maxAge, maxCount, truncateBefore, cacheControl, acl)

  }

//======================================================================================================================
// EventData
//======================================================================================================================

  private[sec] object eventdataGen {

    private def bv(content: String, ct: Content.Type): ByteVector =
      ct.fold(Content.binary(content).map(_.bytes).unsafe, Content.json(content).map(_.bytes).unsafe)

    private def dataBV(id: ju.UUID, ct: Content.Type): ByteVector =
      ct.fold(bv(s"data@$id", ct), bv(s"""{ "data" : "$id" }""", ct))

    private def metaBV(id: ju.UUID, ct: Content.Type, empty: Boolean): ByteVector =
      Option.unless(empty)(ct.fold(bv(s"meta@$id", ct), bv(s"""{ "meta" : "$id" }""", ct))).getOrElse(ByteVector.empty)

    private def eventIdAndType(etPrefix: String): Gen[(ju.UUID, EventType)] =
      for {
        uuid      <- Gen.uuid
        eventType <- eventTypeGen.genEventTypeUserDefined(etPrefix)
      } yield (uuid, eventType)

    val genMeta: Gen[Boolean]    = Gen.oneOf(true, false)
    val genCT: Gen[Content.Type] = Gen.oneOf(Content.Type.Binary, Content.Type.Json)

    def eventDataN(n: Int, etPrefix: String): Gen[List[EventData]] =
      for {
        gm         <- genMeta
        ct         <- genCT
        idAndTypes <- Gen.infiniteStream(eventIdAndType(etPrefix)).flatMap(_.take(n).toList)
        gen        <- idAndTypes
        (id, et)    = gen
        data        = dataBV(id, ct)
        meta        = metaBV(id, ct, gm)
      } yield ct.fold(EventData.binary(et, id, data, meta), EventData.json(et, id, data, meta))

    val eventDataOne: Gen[EventData] = for {
      ct        <- genCT
      idAndType <- eventIdAndType(eventTypeGen.defaultPrefix)
      (id, et)   = idAndType
      data       = dataBV(id, ct)
      meta       = metaBV(id, ct, empty = false)
    } yield ct.fold(EventData.binary(et, id, data, meta), EventData.json(et, id, data, meta))

    @tailrec
    def eventDataNelN(n: Int, etPrefix: String): Gen[NonEmptyList[EventData]] =
      sampleOfGen(eventDataN(math.max(1, n), etPrefix)) match {
        case head :: tail => NonEmptyList[EventData](head, tail)
        case Nil          => eventDataNelN(n, etPrefix)
      }
  }

  def arbEventDataNelOfN(n: Int, etPrefix: String): Arbitrary[NonEmptyList[EventData]] =
    Arbitrary(eventdataGen.eventDataNelN(n, etPrefix))

  implicit val arbEventData: Arbitrary[EventData]                 = Arbitrary(eventdataGen.eventDataOne)
  implicit val arbEventDataNN: Arbitrary[NonEmptyList[EventData]] = arbEventDataNelOfN(25, eventTypeGen.defaultPrefix)

//======================================================================================================================
// Event
//======================================================================================================================

  private[sec] object eventGen {

    val eventRecordOne: Gen[EventRecord] = for {
      sid <- arbStreamIdNormal.arbitrary
      p   <- arbPositionExact.arbitrary
      n   <- arbEventNumberExact.arbitrary.suchThat(_.value < p.commit)
      ed  <- arbEventData.arbitrary
      c   <- arbZonedDateTime.arbitrary
    } yield EventRecord(sid, n, p, ed, c)

    def eventRecordNelN(
      n: Int,
      streamPrefix: Option[String] = "sec-".some,
      etPrefix: Option[String] = eventTypeGen.defaultPrefix.some
    ): Gen[NonEmptyList[EventRecord]] = {

      val sid  = sampleOfGen(idGen.genStreamIdNormal(streamPrefix.getOrElse("")))
      val data = sampleOfGen(eventdataGen.eventDataNelN(n, etPrefix.getOrElse("")))
      val zdt  = sampleOf[ZonedDateTime]

      data.zipWithIndex.map { case (ed, i) =>
        val commit   = i + 1000L
        val position = Position.exact(commit, commit)
        val number   = EventNumber.exact(i.toLong)
        val created  = zdt.plusSeconds(i.toLong)
        EventRecord(sid, number, position, ed, created)
      }
    }

  }

  def arbEventRecordNelOfN(n: Int): Arbitrary[NonEmptyList[EventRecord]] = Arbitrary(eventGen.eventRecordNelN(n))
  implicit val arbEventRecord: Arbitrary[EventRecord]                    = Arbitrary[EventRecord](eventGen.eventRecordOne)
  implicit val arbEventRecordN: Arbitrary[NonEmptyList[EventRecord]]     = arbEventRecordNelOfN(25)

//======================================================================================================================
// Endpoint
//======================================================================================================================

  implicit val arbEndpoint: Arbitrary[Endpoint] =
    Arbitrary(Gen.oneOf(Endpoint("127.0.0.1", 2113), Endpoint("127.0.0.2", 2113), Endpoint("127.0.0.3", 2113)))

//======================================================================================================================
// Gossip
//======================================================================================================================

  implicit val arbVNodeState: Arbitrary[VNodeState] =
    Arbitrary(Gen.oneOf(VNodeState.values))

  implicit val arbMemberInfo: Arbitrary[MemberInfo] = Arbitrary[MemberInfo] {
    for {
      id  <- arbitrary[ju.UUID]
      ts  <- arbitrary[ZonedDateTime]
      vns <- arbitrary[VNodeState]
      al  <- arbitrary[Boolean]
      ep  <- arbitrary[Endpoint]
    } yield MemberInfo(id, ts, vns, al, ep)
  }

  implicit val arbClusterInfo: Arbitrary[ClusterInfo] = Arbitrary[ClusterInfo] {
    Gen.listOfN(5, arbMemberInfo.arbitrary).map(ms => ClusterInfo(ms.toSet))
  }

}
