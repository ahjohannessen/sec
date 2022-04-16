/*
 * Copyright 2020 Scala EventStoreDB Client
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
import scala.collection.immutable.{Nil, SortedSet}
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import cats.data.NonEmptyList
import cats.implicits._
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck._
import scodec.bits.ByteVector
import sec.api._
import sec.helpers.implicits._
import StreamState.{Any, NoStream, StreamExists}

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
// StreamPosition, LogPosition, PositionInfo.Global & StreamState
//======================================================================================================================

  implicit val ulong: Arbitrary[ULong] =
    Arbitrary(arbitrary[Long].map(new ULong(_)))

  implicit val arbStreamPositionExact: Arbitrary[StreamPosition.Exact] =
    Arbitrary[StreamPosition.Exact](arbitrary(ulong).map(StreamPosition.Exact(_)))

  implicit val arbStreamPosition: Arbitrary[StreamPosition] =
    Arbitrary[StreamPosition](Gen.oneOf(List(StreamPosition.End, sampleOf[StreamPosition.Exact])))

  implicit val arbLogPositionExact: Arbitrary[LogPosition.Exact] = Arbitrary[LogPosition.Exact](for {
    c <- Gen.chooseNum(0L, Long.MaxValue)
    p <- Gen.chooseNum(0L, 10L).map(c - _).suchThat(_ >= 0)
  } yield LogPosition.Exact(c, p).leftMap(require(false, _)).toOption.get)

  implicit val arbLogPosition: Arbitrary[LogPosition] =
    Arbitrary[LogPosition](Gen.oneOf(List(LogPosition.End, sampleOf[LogPosition.Exact])))

  implicit val arbPositionInfoGlobal: Arbitrary[PositionInfo.Global] = Arbitrary[PositionInfo.Global](
    arbLogPositionExact.arbitrary.flatMap(lpe =>
      arbStreamPositionExact.arbitrary.suchThat(_.value <= lpe.commit).map(PositionInfo.Global(_, lpe)))
  )

  implicit val arbStreamState: Arbitrary[StreamState] =
    Arbitrary[StreamState](Gen.oneOf(List(NoStream, Any, StreamExists, sampleOf[StreamPosition.Exact])))

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

  implicit val arbStreamIdSystem: Arbitrary[StreamId.System] = Arbitrary[StreamId.System] {
    import StreamId._
    Gen.oneOf(All, Settings, Stats, Scavenges, Streams, System.unsafe(sampleOf[StreamId.Normal].name))
  }

  implicit val arbStreamIdId: Arbitrary[StreamId.Id] =
    Arbitrary[StreamId.Id](Gen.oneOf(sampleOf[StreamId.Normal], sampleOf[StreamId.System]))

  implicit val arbStreamIdMetaId: Arbitrary[StreamId.MetaId] =
    Arbitrary[StreamId.MetaId](Gen.oneOf(sampleOf[StreamId.System], sampleOf[StreamId.Normal]).map(_.metaId))

  implicit val arbStreamId: Arbitrary[StreamId] =
    Arbitrary[StreamId](Gen.oneOf(sampleOf[StreamId.Id], sampleOf[StreamId.MetaId]))

//======================================================================================================================
// EventType
//======================================================================================================================

  private[sec] object eventTypeGen {

    val defaultPrefix = "com.eventstore.client.Event"

    def genEventTypeUserDefined(prefix: String): Gen[EventType.Normal] = {

      val gen: Gen[String] = for {
        c  <- Gen.alphaUpperChar
        cs <- Gen.listOfN(2, Gen.alphaLowerChar)
      } yield s"$prefix${(c :: cs).mkString}"

      gen.map(et => EventType.normal(et).unsafe)
    }

  }

  implicit val arbEventTypeUserDefined: Arbitrary[EventType.Normal] = {
    import eventTypeGen._
    Arbitrary[EventType.Normal](genEventTypeUserDefined(defaultPrefix))
  }

  implicit val arbEventTypeSysteDefined: Arbitrary[EventType.System] = Arbitrary[EventType.System] {
    import EventType._
    Gen.oneOf(
      StreamDeleted,
      StatsCollected,
      LinkTo,
      StreamReference,
      StreamMetadata,
      Settings,
      System.unsafe(sampleOf[Normal].name)
    )
  }

  implicit val arbEventType: Arbitrary[EventType] =
    Arbitrary[EventType](Gen.oneOf(sampleOf[EventType.System], sampleOf[EventType.Normal]))

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

  implicit val arbMetaState: Arbitrary[MetaState] = Arbitrary[MetaState] {

    val oneYear = 31536000L
    val seconds = Gen.chooseNum(1L, oneYear).map(FiniteDuration(_, SECONDS))

    for {
      maxAge         <- Gen.option(seconds.map(MaxAge(_).unsafe))
      maxCount       <- Gen.option(Gen.chooseNum(1, Int.MaxValue).map(MaxCount(_).unsafe))
      truncateBefore <- Gen.option(arbStreamPositionExact.arbitrary.suchThat(_ > StreamPosition.Start))
      cacheControl   <- Gen.option(seconds.map(CacheControl(_).unsafe))
      acl            <- Gen.option(arbStreamAcl.arbitrary)
    } yield MetaState(maxAge, maxCount, truncateBefore, cacheControl, acl)

  }

//======================================================================================================================
// EventData
//======================================================================================================================

  private[sec] object eventdataGen {

    private def encode(data: String): ByteVector =
      helpers.text.encodeToBV(data).unsafe

    private def dataBV(id: ju.UUID, ct: ContentType): ByteVector =
      ct.fold(encode(s"data@$id"), encode(s"""{ "data" : "$id" }"""))

    private def metaBV(id: ju.UUID, ct: ContentType, empty: Boolean): ByteVector =
      Option.unless(empty)(ct.fold(encode(s"meta@$id"), encode(s"""{ "meta" : "$id" }"""))).getOrElse(ByteVector.empty)

    private def eventIdAndType(etPrefix: String): Gen[(ju.UUID, EventType)] =
      for {
        uuid      <- Gen.uuid
        eventType <- eventTypeGen.genEventTypeUserDefined(etPrefix)
      } yield (uuid, eventType)

    val genMeta: Gen[Boolean]   = Gen.oneOf(true, false)
    val genCT: Gen[ContentType] = Gen.oneOf(ContentType.Binary, ContentType.Json)

    def eventDataN(n: Int, etPrefix: String): Gen[List[EventData]] =
      for {
        gm         <- genMeta
        ct         <- genCT
        idAndTypes <- Gen.infiniteLazyList(eventIdAndType(etPrefix)).flatMap(_.take(n).toList)
        gen        <- idAndTypes
        (id, et)    = gen
        data        = dataBV(id, ct)
        meta        = metaBV(id, ct, gm)
      } yield EventData(et, id, data, meta, ct)

    val eventDataOne: Gen[EventData] = for {
      ct        <- genCT
      idAndType <- eventIdAndType(eventTypeGen.defaultPrefix)
      (id, et)   = idAndType
      data       = dataBV(id, ct)
      meta       = metaBV(id, ct, empty = false)
    } yield EventData(et, id, data, meta, ct)

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

  type AllEventRecord    = EventRecord[PositionInfo.Global]
  type StreamEventRecord = EventRecord[PositionInfo.Local]

  private[sec] object eventGen {

    val streamEventRecordOne: Gen[StreamEventRecord] = for {
      sid <- arbStreamIdNormal.arbitrary
      sp  <- arbStreamPositionExact.arbitrary
      ed  <- arbEventData.arbitrary
      c   <- arbZonedDateTime.arbitrary
    } yield sec.EventRecord(sid, sp, ed, c)

    val allEventRecordOne: Gen[AllEventRecord] = for {
      sid <- arbStreamIdNormal.arbitrary
      pa  <- arbPositionInfoGlobal.arbitrary
      ed  <- arbEventData.arbitrary
      c   <- arbZonedDateTime.arbitrary
    } yield sec.EventRecord(sid, pa, ed, c)

    def streamEventRecordNelN(
      n: Int,
      streamPrefix: Option[String] = "sec-".some,
      etPrefix: Option[String] = eventTypeGen.defaultPrefix.some
    ): Gen[NonEmptyList[StreamEventRecord]] = {

      val sid  = sampleOfGen(idGen.genStreamIdNormal(streamPrefix.getOrElse("")))
      val data = sampleOfGen(eventdataGen.eventDataNelN(n, etPrefix.getOrElse("")))
      val zdt  = sampleOf[ZonedDateTime]

      data.zipWithIndex.map { case (ed, i) =>
        val position = StreamPosition(i.toLong)
        val created  = zdt.plusSeconds(i.toLong)
        sec.EventRecord[PositionInfo.Local](sid, position, ed, created)
      }
    }

  }

  def arbStreamEventRecordNelOfN(n: Int): Arbitrary[NonEmptyList[StreamEventRecord]] = Arbitrary(
    eventGen.streamEventRecordNelN(n))

  implicit val arbAllEventRecord: Arbitrary[AllEventRecord] =
    Arbitrary[AllEventRecord](eventGen.allEventRecordOne)

  implicit val arbStreamEventRecord: Arbitrary[StreamEventRecord] =
    Arbitrary[StreamEventRecord](eventGen.streamEventRecordOne)

  implicit val arbStreamEventRecordN: Arbitrary[NonEmptyList[StreamEventRecord]] =
    arbStreamEventRecordNelOfN(25)

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
