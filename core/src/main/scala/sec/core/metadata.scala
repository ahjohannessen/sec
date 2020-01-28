package sec
package core

import scala.concurrent.duration._
import cats.Show
import cats.Endo
import cats.implicits._
import io.circe._
import io.circe.syntax._
import io.circe.Decoder.Result

//======================================================================================================================

private[sec] final case class StreamMetadata(
  state: StreamState,
  custom: Option[JsonObject]
)

private[sec] object StreamMetadata {

  final val empty: StreamMetadata               = StreamMetadata(StreamState.empty)
  def apply(state: StreamState): StreamMetadata = StreamMetadata(state, None)

  implicit final class StreamMetadataOps(val sm: StreamMetadata) extends AnyVal {

    def modifyState(fn: StreamState => StreamState): StreamMetadata =
      sm.copy(state = fn(sm.state))

    def withTruncateBefore(tb: Option[EventNumber.Exact]): StreamMetadata =
      sm.modifyState(_.copy(truncateBefore = tb))

    def withMaxAge(ma: Option[MaxAge]): StreamMetadata =
      sm.modifyState(_.copy(maxAge = ma))

    def withMaxCount(mc: Option[MaxCount]): StreamMetadata =
      sm.modifyState(_.copy(maxCount = mc))

    def withCacheControl(cc: Option[CacheControl]): StreamMetadata =
      sm.modifyState(_.copy(cacheControl = cc))

    def withAcl(sa: Option[StreamAcl]): StreamMetadata =
      sm.modifyState(_.copy(acl = sa))

    ///

    def decodeCustom[F[_]: ErrorA, A: Decoder]: F[Option[A]] =
      sm.custom.traverse(jo => Decoder[A].apply(Json.fromJsonObject(jo).hcursor).liftTo[F])

    def modifyCustom[F[_]: ErrorA, A: Codec.AsObject](fn: Endo[Option[A]]): F[StreamMetadata] =
      decodeCustom[F, A].map(c => sm.copy(custom = fn(c).map(Encoder.AsObject[A].encodeObject)))

  }

  ///

  import StreamState.metadataKeys

  final val reservedKeys: Set[String] = Set(
    metadataKeys.MaxAge,
    metadataKeys.TruncateBefore,
    metadataKeys.MaxCount,
    metadataKeys.Acl,
    metadataKeys.CacheControl
  )

  implicit val codecForStreamMetadata: Codec.AsObject[StreamMetadata] = new Codec.AsObject[StreamMetadata] {

    val encodeSS: StreamState => JsonObject         = Encoder.AsObject[StreamState].encodeObject(_)
    val decodeSS: JsonObject => Result[StreamState] = jo => Decoder[StreamState].apply(jo.asJson.hcursor)

    def encodeObject(sm: StreamMetadata): JsonObject = sm.custom match {
      case Some(c) if c.nonEmpty => encodeSS(sm.state).toList.foldLeft(c) { case (i, (k, v)) => i.add(k, v) }
      case _                     => encodeSS(sm.state)
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

sealed abstract case class MaxAge(value: FiniteDuration)
object MaxAge {

  /**
   * @param maxAge must be greater than or equal to 1 second.
   * @return [[Attempt[MaxAge]]]
   */
  def from(maxAge: FiniteDuration): Attempt[MaxAge] =
    if (maxAge < 1.second) s"maxAge must be >= 1 second, it was $maxAge.".asLeft
    else new MaxAge(maxAge) {}.asRight

  def apply[F[_]: ErrorA](maxAge: FiniteDuration): F[MaxAge] =
    from(maxAge).orFail[F](ValidationError)

  implicit val showForMaxAge: Show[MaxAge] = Show.show(_.value.toString())
}

sealed abstract case class MaxCount(value: Int)
object MaxCount {

  /**
   * @param maxCount must be greater than or equal to 1.
   * @return [[Attempt[MaxCount]]]
   */
  def from(maxCount: Int): Attempt[MaxCount] =
    if (maxCount < 1) s"max count must be >= 1, it was $maxCount.".asLeft
    else new MaxCount(maxCount) {}.asRight

  def apply[F[_]: ErrorA](maxCount: Int): F[MaxCount] =
    from(maxCount).orFail[F](ValidationError)

  implicit val showForMaxCount: Show[MaxCount] = Show.show { mc =>
    s"${mc.value} event${if (mc.value == 1) "" else "s"}"
  }
}

sealed abstract case class CacheControl(value: FiniteDuration)
object CacheControl {

  /**
   * @param cacheControl must be greater than or equal to 1 second.
   * @return [[Attempt[CacheControl]]]
   */
  def from(cacheControl: FiniteDuration): Attempt[CacheControl] =
    if (cacheControl < 1.second) s"cache control must be >= 1, it was $cacheControl.".asLeft
    else new CacheControl(cacheControl) {}.asRight

  def apply[F[_]: ErrorA](maxAge: FiniteDuration): F[CacheControl] =
    from(maxAge).orFail[F](ValidationError)

  implicit val showForCacheControl: Show[CacheControl] = Show.show(_.value.toString())
}

//======================================================================================================================

/**
 * @param maxAge The maximum age of events in the stream.
 * Items older than this will be automatically removed.
 *
 * @param maxCount The maximum count of events in the stream.
 * When you have more than count the oldest will be removed.
 *
 * @param truncateBefore When set says that items prior to event 'E' can
 * be truncated and will be removed.
 *
 * @param cacheControl The head of a feed in the atom api is not cacheable.
 * This allows you to specify a period of time you want it to be cacheable.
 * Low numbers are best here (say 30-60 seconds) and introducing values
 * here will introduce latency over the atom protocol if caching is occuring.
 *
 * @param acl The access control list for this stream.
 *
 *
 * @note More details are here https://eventstore.org/docs/server/deleting-streams-and-events/index.html
 *
 * */
private[sec] final case class StreamState(
  maxAge: Option[MaxAge],
  maxCount: Option[MaxCount],
  truncateBefore: Option[EventNumber.Exact],
  cacheControl: Option[CacheControl],
  acl: Option[StreamAcl]
)

private[sec] object StreamState {

  val empty: StreamState = StreamState(None, None, None, None, None)

  ///

  private[sec] implicit val codecForStreamMetadata: Codec.AsObject[StreamState] =
    new Codec.AsObject[StreamState] {

      import Decoder.{decodeInt => di, decodeLong => dl}
      import Encoder.{encodeInt => ei, encodeLong => el}

      private final val cfd: Codec[FiniteDuration] =
        Codec.from(dl.map(FiniteDuration(_, SECONDS)), el.contramap(_.toSeconds))

      implicit val codecForMaxAge: Codec[MaxAge] =
        Codec.from(cfd.emap(MaxAge.from), cfd.contramap(_.value))

      implicit val codecForMaxCount: Codec[MaxCount] =
        Codec.from(di.emap(MaxCount.from), ei.contramap(_.value))

      implicit val codecForCacheControl: Codec[CacheControl] =
        Codec.from(cfd.emap(CacheControl.from), cfd.contramap(_.value))

      implicit val codecForEventNumber: Codec[EventNumber.Exact] =
        Codec.from(dl.map(EventNumber.exact), el.contramap(_.value))

      //

      def encodeObject(a: StreamState): JsonObject = {

        val data = Map(
          metadataKeys.MaxAge         -> a.maxAge.asJson,
          metadataKeys.TruncateBefore -> a.truncateBefore.asJson,
          metadataKeys.MaxCount       -> a.maxCount.asJson,
          metadataKeys.Acl            -> a.acl.asJson,
          metadataKeys.CacheControl   -> a.cacheControl.asJson
        )

        JsonObject.fromMap(data).mapValues(_.dropNullValues)
      }

      def apply(c: HCursor): Result[StreamState] =
        for {

          maxAge         <- c.get[Option[MaxAge]](metadataKeys.MaxAge)
          truncateBefore <- c.get[Option[EventNumber.Exact]](metadataKeys.TruncateBefore)
          maxCount       <- c.get[Option[MaxCount]](metadataKeys.MaxCount)
          acl            <- c.get[Option[StreamAcl]](metadataKeys.Acl)
          cacheControl   <- c.get[Option[CacheControl]](metadataKeys.CacheControl)

        } yield StreamState(maxAge, maxCount, truncateBefore, cacheControl, acl)

    }

  private[sec] object metadataKeys {

    final val MaxAge: String         = "$maxAge"
    final val MaxCount: String       = "$maxCount"
    final val TruncateBefore: String = "$tb"
    final val CacheControl: String   = "$cacheControl"
    final val Acl: String            = "$acl"

  }

  implicit val showForStreamState: Show[StreamState] = Show.show[StreamState] { ss =>
    s"""
       |StreamState:
       |  max-age         = ${ss.maxAge.map(_.show).getOrElse("n/a")}
       |  max-count       = ${ss.maxCount.map(_.show).getOrElse("n/a")}
       |  cache-control   = ${ss.cacheControl.map(_.show).getOrElse("n/a")}
       |  truncate-before = ${ss.truncateBefore.map(_.show).getOrElse("n/a")}
       |  access-list     = ${ss.acl.map(_.show).getOrElse("n/a")}
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

    def encodeObject(a: StreamAcl): JsonObject = {

      val roles: Map[String, Set[String]] = Map(
        aclKeys.Read      -> a.readRoles,
        aclKeys.Write     -> a.writeRoles,
        aclKeys.Delete    -> a.deleteRoles,
        aclKeys.MetaRead  -> a.metaReadRoles,
        aclKeys.MetaWrite -> a.metaWriteRoles
      )

      val nonEmptyRoles = roles.collect {
        case (k, v) if v.nonEmpty => k -> v.asJson
      }

      JsonObject.fromMap(nonEmptyRoles)
    }

    def apply(c: HCursor): Result[StreamAcl] = {

      def get(k: String): Result[Set[String]] =
        c.getOrElse[Set[String]](k)(Set.empty)(Decoder[Set[String]].or(Decoder[String].map(Set(_))))

      (get(aclKeys.Read), get(aclKeys.Write), get(aclKeys.Delete), get(aclKeys.MetaRead), get(aclKeys.MetaWrite))
        .mapN(StreamAcl.apply)
    }
  }

  private[sec] object aclKeys {

    final val Read: String            = "$r"
    final val Write: String           = "$w"
    final val Delete: String          = "$d"
    final val MetaRead: String        = "$mr"
    final val MetaWrite: String       = "$mw"
    final val UserStreamAcl: String   = "$userStreamAcl"
    final val SystemStreamAcl: String = "$systemStreamAcl"

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
