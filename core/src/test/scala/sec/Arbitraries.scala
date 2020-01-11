package sec

import scala.annotation.tailrec
import cats.implicits._
import org.scalacheck._
import sec.core._
import sec.core.StreamRevision.{Any, NoStream, StreamExists}

object Arbitraries {

  @tailrec
  final def sampleOf[T](implicit ev: Arbitrary[T]): T =
    ev.arbitrary.sample match {
      case Some(t) => t
      case None    => sampleOf[T]
    }

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

  implicit val arbStreamIdNormal: Arbitrary[StreamId.Normal] = Arbitrary[StreamId.Normal](
    Gen.asciiStr.suchThat(s => s.nonEmpty && !s.startsWith(StreamId.systemPrefix)).map(StreamId.normal).map(_.unsafe)
  )

  implicit val arbStreamIdSystem: Arbitrary[StreamId.System] = Arbitrary[StreamId.System](
    Arbitrary.arbitrary[StreamId.Normal].map(n => StreamId.system(n.name)).map(_.unsafe)
  )

  implicit val arbStreamIdNormalId: Arbitrary[StreamId.NormalId] =
    Arbitrary(sampleOf[StreamId.NormalId])

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

}
