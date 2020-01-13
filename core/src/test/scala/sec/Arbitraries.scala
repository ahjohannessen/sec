package sec

import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import scala.annotation.tailrec
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import cats.implicits._
import sec.core._
import sec.core.StreamRevision.{Any, NoStream, StreamExists}
import org.scalacheck._

object Arbitraries {

  @tailrec
  final def sampleOf[T](implicit ev: Arbitrary[T]): T =
    ev.arbitrary.sample match {
      case Some(t) => t
      case None    => sampleOf[T]
    }

//======================================================================================================================
// Common Std Instances
//======================================================================================================================

  implicit val arbLocalDate: Arbitrary[LocalDate] = Arbitrary(
    Gen.choose(-1000L, 1000L).map(LocalDate.now(ZoneOffset.UTC).plusDays(_))
  )

  implicit val arbZonedDateTime: Arbitrary[ZonedDateTime] = Arbitrary(
    Gen.choose(-86400000L, 86400000L).map(ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(_))
  )

//======================================================================================================================
// EventNumber & Position
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

  implicit val arbStreamIdNormal: Arbitrary[StreamId.Normal] = Arbitrary[StreamId.Normal](
    Gen.asciiStr.suchThat(s => s.nonEmpty && !s.startsWith(StreamId.systemPrefix)).map(n => StreamId.normal(n).unsafe)
  )

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
    Arbitrary[StreamId.MetaId](Gen.oneOf(sampleOf[StreamId.SystemId], sampleOf[StreamId.Normal]).map(_.meta))

  implicit val arbStreamId: Arbitrary[StreamId] =
    Arbitrary[StreamId](Gen.oneOf(sampleOf[StreamId.Id], sampleOf[StreamId.MetaId]))

//======================================================================================================================
// EventType
//======================================================================================================================

  implicit val arbEventTypeUserDefined: Arbitrary[EventType.UserDefined] = Arbitrary {
    import EventType._
    Gen.asciiStr.suchThat(s => s.nonEmpty && !s.startsWith(systemPrefix)).map(n => userDefined(n).unsafe)
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

  implicit val arbStreamAcl: Arbitrary[StreamAcl] = Arbitrary {

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

  implicit val arbStreamState: Arbitrary[StreamState] = Arbitrary {

    val oneYear = 31536000L
    val seconds = Gen.chooseNum(1L, oneYear).map(FiniteDuration(_, SECONDS))

    for {

      maxAge         <- Gen.option(seconds)
      maxCount       <- Gen.option(Gen.chooseNum(1, Int.MaxValue))
      truncateBefore <- Gen.option(arbEventNumberExact.arbitrary.suchThat(_ > EventNumber.Start))
      cacheControl   <- Gen.option(seconds)
      acl            <- Gen.option(arbStreamAcl.arbitrary)

    } yield {
      StreamState(maxAge, maxCount, truncateBefore, cacheControl, acl)
    }
  }

}
