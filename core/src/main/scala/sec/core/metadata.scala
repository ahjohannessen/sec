package sec
package core

import scala.concurrent.duration._
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

private[sec] final case class StreamState(
  maxAge: Option[FiniteDuration],
  truncateBefore: Option[EventNumber],
  maxCount: Option[Int],
  acl: Option[StreamAcl],
  cacheControl: Option[FiniteDuration]
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

        } yield StreamState(maxAge, truncateBefore, maxCount, acl, cacheControl)

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
}
