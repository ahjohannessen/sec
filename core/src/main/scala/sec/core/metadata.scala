package sec
package core

import scala.concurrent.duration._
import cats.Show
import cats.implicits._
import io.circe._
import io.circe.syntax._
import io.circe.Decoder.Result
import constants.SystemMetadata._

//======================================================================================================================

private[sec] final case class StreamMetadata(
  settings: StreamState,
  custom: Option[JsonObject]
)

private[sec] object StreamMetadata {

  def apply(settings: StreamState): StreamMetadata = StreamMetadata(settings, None)

  ///

  final val reservedKeys: Set[String] = Set(MaxAge, TruncateBefore, MaxCount, Acl, CacheControl)

  implicit val codecForStreamMetadata: Codec.AsObject[StreamMetadata] = new Codec.AsObject[StreamMetadata] {

    val encodeSS: StreamState => JsonObject         = Encoder.AsObject[StreamState].encodeObject(_)
    val decodeSS: JsonObject => Result[StreamState] = jo => Decoder[StreamState].apply(jo.asJson.hcursor)

    def encodeObject(sm: StreamMetadata): JsonObject = sm.custom match {
      case Some(c) if c.nonEmpty => encodeSS(sm.settings).toList.foldLeft(c) { case (i, (k, v)) => i.add(k, v) }
      case _                     => encodeSS(sm.settings)
    }

    def apply(c: HCursor): Result[StreamMetadata] =
      Decoder.decodeJsonObject(c).map { both =>
        val (ours, theirs) = both.toList.partition(kv => reservedKeys.contains(kv._1))
        (JsonObject.fromIterable(ours), JsonObject.fromIterable(theirs))
      } >>= {
        case (s, c) => decodeSS(s).map(StreamMetadata(_, Option.when(c.nonEmpty)(c)))
      }
  }

}

//======================================================================================================================

/**
 * @param maxAge The maximum age of events in the stream.
 * Items older than this will be automatically removed.
 * This value must be >= 1 second.
 *
 * @param maxCount The maximum count of events in the stream.
 * When you have more than count the oldest will be removed.
 * This value must be >= 1.
 *
 * @param truncateBefore When set says that items prior to event 'E' can
 * be truncated and will be removed.
 *
 * @param cacheControl The head of a feed in the atom api is not cacheable.
 * This allows you to specify a period of time you want it to be cacheable.
 * Low numbers are best here (say 30-60 seconds) and introducing values
 * here will introduce latency over the atom protocol if caching is occuring.
 * This value must be >= 1 second.
 *
 * @param acl The access control list for this stream.
 * */
//
// TODO: If this type should be surfaced then either newtypes or builder that ensure valid values is needed.
// Include some details from https://eventstore.org/docs/server/deleting-streams-and-events/index.html

private[sec] final case class StreamState(
  maxAge: Option[FiniteDuration],       // newtype that ensures >=1s
  maxCount: Option[Int],                // newtype that ensures >=1
  truncateBefore: Option[EventNumber],  // EventNumber.Start valid? If invalid then >= Start, e.g. 0 => None
  cacheControl: Option[FiniteDuration], // newtype that ensures >=1s
  acl: Option[StreamAcl]
)

private[sec] object StreamState {

  val empty: StreamState = StreamState(None, None, None, None, None)

  ///

  private[sec] implicit val codecForStreamMetadata: Codec.AsObject[StreamState] =
    new Codec.AsObject[StreamState] {

      implicit val codecForFiniteDuration: Codec[FiniteDuration] =
        Codec.from(Decoder.decodeLong.map(l => FiniteDuration(l, SECONDS)), Encoder[Long].contramap(_.toSeconds))

      implicit val codecForEventNumber: Codec[EventNumber] =
        Codec.from(Decoder.decodeLong.map(EventNumber(_)), Encoder[Long].contramap { case EventNumber(v) => v })

      def encodeObject(a: StreamState): JsonObject = {

        val data = Map(
          MaxAge         -> a.maxAge.asJson,
          TruncateBefore -> a.truncateBefore.asJson,
          MaxCount       -> a.maxCount.asJson,
          Acl            -> a.acl.asJson,
          CacheControl   -> a.cacheControl.asJson
        )

        JsonObject.fromMap(data).mapValues(_.dropNullValues)
      }

      def apply(c: HCursor): Result[StreamState] =
        for {

          maxAge         <- c.get[Option[FiniteDuration]](MaxAge)
          truncateBefore <- c.get[Option[EventNumber]](TruncateBefore)
          maxCount       <- c.get[Option[Int]](MaxCount)
          acl            <- c.get[Option[StreamAcl]](Acl)
          cacheControl   <- c.get[Option[FiniteDuration]](CacheControl)

        } yield StreamState(maxAge, maxCount, truncateBefore, cacheControl, acl)

    }

  implicit val showForStreamState: Show[StreamState] = Show.show[StreamState] { ss =>
    s"""
       |StreamState:
       |  max-age         = ${ss.maxAge.getOrElse(" - ")}
       |  max-count       = ${ss.maxCount.map(c => if (c == 1) s"$c event" else s"$c events").getOrElse(" - ")}
       |  cache-control   = ${ss.cacheControl.getOrElse(" - ")}
       |  truncate-before = ${ss.truncateBefore.map(_.show).getOrElse(" - ")}
       |  access-list     = ${ss.acl.map(_.show).getOrElse(" - ")}
       |""".stripMargin

  }

}

//======================================================================================================================

/**
 * @param readRoles Roles and users permitted to read the stream.
 * @param writeRoles Roles and users permitted to write to the stream.
 * @param deleteRoles Roles and users permitted to delete the stream.
 * @param metaReadRoles Roles and users permitted to read stream metadata.
 * @param metaWriteRoles Roles and users permitted to write stream metadata.
 * */
final case class StreamAcl(
  readRoles: Set[String],
  writeRoles: Set[String],
  deleteRoles: Set[String],
  metaReadRoles: Set[String],
  metaWriteRoles: Set[String]
)

object StreamAcl {

  final val empty: StreamAcl = StreamAcl(Set.empty, Set.empty, Set.empty, Set.empty, Set.empty)

  ///

  private[sec] implicit val codecForStreamAcl: Codec.AsObject[StreamAcl] = new Codec.AsObject[StreamAcl] {

    import constants.SystemMetadata.AclKeys._

    def encodeObject(a: StreamAcl): JsonObject = {

      val roles: Map[String, Set[String]] = Map(
        Read      -> a.readRoles,
        Write     -> a.writeRoles,
        Delete    -> a.deleteRoles,
        MetaRead  -> a.metaReadRoles,
        MetaWrite -> a.metaWriteRoles
      )

      val nonEmptyRoles = roles.collect {
        case (k, v) if v.nonEmpty => k -> v.asJson
      }

      JsonObject.fromMap(nonEmptyRoles)
    }

    def apply(c: HCursor): Result[StreamAcl] = {

      def get(k: String): Result[Set[String]] =
        c.getOrElse[Set[String]](k)(Set.empty)(Decoder[Set[String]].or(Decoder[String].map(Set(_))))

      (get(Read), get(Write), get(Delete), get(MetaRead), get(MetaWrite)).mapN(StreamAcl.apply)
    }
  }

  implicit val showForStreamAcl: Show[StreamAcl] = Show.show[StreamAcl] { ss =>
    def show(label: String, roles: Set[String]): String =
      s"$label: ${roles.mkString("[", ", ", "]")}"

    val r  = show("read", ss.readRoles)
    val w  = show("write", ss.writeRoles)
    val d  = show("delete", ss.deleteRoles)
    val mr = show("meta-read", ss.metaReadRoles)
    val mw = show("meta-write", ss.metaWriteRoles)

    s"$r, $w, $d, $mr, $mw"
  }

}
