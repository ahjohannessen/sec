package sec.core

import scala.concurrent.duration._
import io.circe._
import constants.SystemMetadata._

private[sec] final case class StreamMetaData(
  maxAge: Option[FiniteDuration],
  truncateBefore: Option[EventNumber],
  maxCount: Option[Int],
  acl: Option[StreamAcl],
  cacheControl: Option[FiniteDuration]
)

private[sec] object StreamMetaData {

  val empty: StreamMetaData = StreamMetaData(None, None, None, None, None)

  private implicit val codecForFiniteDuration: Codec[FiniteDuration] =
    Codec.from(Decoder.decodeLong.map(l => FiniteDuration(l, SECONDS)), Encoder[Long].contramap(_.toSeconds))

  private implicit val codecForEventNumber: Codec[EventNumber] =
    Codec.from(Decoder.decodeLong.map(EventNumber(_)), Encoder[Long].contramap { case EventNumber(v) => v })

  implicit val codecForStreamMetaData: Codec[StreamMetaData] = Codec.forProduct5[
    StreamMetaData,
    Option[FiniteDuration],
    Option[EventNumber],
    Option[Int],
    Option[StreamAcl],
    Option[FiniteDuration]
  ](MaxAge, TruncateBefore, MaxCount, Acl.Acl, CacheControl)(
    (ma, tb, mc, sa, cc) => StreamMetaData(ma, tb, mc, sa, cc))(
    smd => (smd.maxAge, smd.truncateBefore, smd.maxCount, smd.acl, smd.cacheControl)
  )
}
