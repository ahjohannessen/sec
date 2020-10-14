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

import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import cats.syntax.all._
import cats.{Endo, Show}
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._

//======================================================================================================================

final private[sec] case class StreamMetadata(
  state: MetaState,
  custom: Option[JsonObject]
)

private[sec] object StreamMetadata {

  final val empty: StreamMetadata             = StreamMetadata(MetaState.empty)
  def apply(state: MetaState): StreamMetadata = StreamMetadata(state, None)

  implicit final class StreamMetadataOps(val sm: StreamMetadata) extends AnyVal {

    def truncateBefore: Option[StreamPosition.Exact] = sm.state.truncateBefore
    def maxAge: Option[MaxAge]                       = sm.state.maxAge
    def maxCount: Option[MaxCount]                   = sm.state.maxCount
    def cacheControl: Option[CacheControl]           = sm.state.cacheControl
    def acl: Option[StreamAcl]                       = sm.state.acl

    ///

    private def modS(fn: MetaState => MetaState): StreamMetadata = sm.copy(state = fn(sm.state))

    def setTruncateBefore(value: Option[StreamPosition.Exact]): StreamMetadata = modS(_.copy(truncateBefore = value))
    def setMaxAge(value: Option[MaxAge]): StreamMetadata                       = modS(_.copy(maxAge = value))
    def setMaxCount(value: Option[MaxCount]): StreamMetadata                   = modS(_.copy(maxCount = value))
    def setCacheControl(value: Option[CacheControl]): StreamMetadata           = modS(_.copy(cacheControl = value))
    def setAcl(value: Option[StreamAcl]): StreamMetadata                       = modS(_.copy(acl = value))
    def setCustom(value: Option[JsonObject]): StreamMetadata                   = sm.copy(custom = value)

    def withTruncateBefore(tb: StreamPosition.Exact): StreamMetadata = sm.setTruncateBefore(tb.some)
    def withMaxAge(value: MaxAge): StreamMetadata                    = sm.setMaxAge(value.some)
    def withMaxCount(value: MaxCount): StreamMetadata                = sm.setMaxCount(value.some)
    def withCacheControl(value: CacheControl): StreamMetadata        = sm.setCacheControl(value.some)
    def withAcl(value: StreamAcl): StreamMetadata                    = sm.setAcl(value.some)
    def withCustom(value: JsonObject): StreamMetadata                = sm.setCustom(value.some)
    def withCustom(value: (String, Json)*): StreamMetadata           = sm.withCustom(JsonObject.fromMap(Map.from(value)))

    ///

    def getCustom[F[_]: ErrorA, T: Decoder]: F[Option[T]] =
      sm.custom.traverse(jo => Decoder[T].apply(Json.fromJsonObject(jo).hcursor).liftTo[F])

    def setCustom[T: Encoder.AsObject](custom: T): StreamMetadata =
      sm.withCustom(Encoder.AsObject[T].encodeObject(custom))

    def modifyCustom[F[_]: ErrorA, T: Codec.AsObject](fn: Endo[Option[T]]): F[StreamMetadata] =
      getCustom[F, T].map(c => sm.setCustom(fn(c).map(Encoder.AsObject[T].encodeObject)))

  }

  ///

  import MetaState.metadataKeys

  final val reservedKeys: Set[String] = Set(
    metadataKeys.MaxAge,
    metadataKeys.TruncateBefore,
    metadataKeys.MaxCount,
    metadataKeys.Acl,
    metadataKeys.CacheControl
  )

  implicit val codecForStreamMetadata: Codec.AsObject[StreamMetadata] = new Codec.AsObject[StreamMetadata] {

    val encodeSS: MetaState => JsonObject         = Encoder.AsObject[MetaState].encodeObject(_)
    val decodeSS: JsonObject => Result[MetaState] = jo => Decoder[MetaState].apply(jo.asJson.hcursor)

    def encodeObject(sm: StreamMetadata): JsonObject = sm.custom match {
      case Some(c) if c.nonEmpty => encodeSS(sm.state).toList.foldLeft(c) { case (i, (k, v)) => i.add(k, v) }
      case _                     => encodeSS(sm.state)
    }

    def apply(c: HCursor): Result[StreamMetadata] =
      Decoder.decodeJsonObject(c).map { both =>
        val (ours, theirs) = both.toList.partition(kv => reservedKeys.contains(kv._1))
        (JsonObject.fromIterable(ours), JsonObject.fromIterable(theirs))
      } >>= { case (s, c) =>
        decodeSS(s).map(StreamMetadata(_, Option.when(c.nonEmpty)(c)))
      }
  }

}

//======================================================================================================================

sealed abstract case class MaxAge(value: FiniteDuration)
object MaxAge {

  /**
   * @param maxAge must be greater than or equal to 1 second.
   */
  def apply(maxAge: FiniteDuration): Attempt[MaxAge] =
    if (maxAge < 1.second) s"maxAge must be >= 1 second, it was $maxAge.".asLeft
    else new MaxAge(maxAge) {}.asRight

  def of[F[_]: ErrorA](maxAge: FiniteDuration): F[MaxAge] =
    MaxAge(maxAge).orFail[F](InvalidMaxAge)

  implicit val showForMaxAge: Show[MaxAge] = Show.show(_.value.toString())

  final case class InvalidMaxAge(msg: String) extends RuntimeException(msg) with NoStackTrace
}

sealed abstract case class MaxCount(value: Int)
object MaxCount {

  /**
   * @param maxCount must be greater than or equal to 1.
   */
  def apply(maxCount: Int): Attempt[MaxCount] =
    if (maxCount < 1) s"max count must be >= 1, it was $maxCount.".asLeft
    else new MaxCount(maxCount) {}.asRight

  def of[F[_]: ErrorA](maxCount: Int): F[MaxCount] =
    MaxCount(maxCount).orFail[F](InvalidMaxCount)

  implicit val showForMaxCount: Show[MaxCount] = Show.show { mc =>
    s"${mc.value} event${if (mc.value == 1) "" else "s"}"
  }

  final case class InvalidMaxCount(msg: String) extends RuntimeException(msg) with NoStackTrace

}

sealed abstract case class CacheControl(value: FiniteDuration)
object CacheControl {

  /**
   * @param cacheControl must be greater than or equal to 1 second.
   */
  def apply(cacheControl: FiniteDuration): Attempt[CacheControl] =
    if (cacheControl < 1.second) s"cache control must be >= 1, it was $cacheControl.".asLeft
    else new CacheControl(cacheControl) {}.asRight

  def of[F[_]: ErrorA](cacheControl: FiniteDuration): F[CacheControl] =
    CacheControl(cacheControl).orFail[F](InvalidCacheControl)

  implicit val showForCacheControl: Show[CacheControl] = Show.show(_.value.toString())

  final case class InvalidCacheControl(msg: String) extends RuntimeException(msg) with NoStackTrace
}

//======================================================================================================================

/**
 * @param maxAge The maximum age of events in the stream.
 * Items older than this will be automatically removed.
 *
 * @param maxCount The maximum count of events in the stream.
 * When you have more than count the oldest will be removed.
 *
 * @param truncateBefore When set says that events with a stream position
 * less than the truncated before value should be removed.
 *
 * @param cacheControl The head of a feed in the atom api is not cacheable.
 * This allows you to specify a period of time you want it to be cacheable.
 * Low numbers are best here (say 30-60 seconds) and introducing values
 * here will introduce latency over the atom protocol if caching is occuring.
 *
 * @param acl The access control list for this stream.
 *
 * @note More details are here https://eventstore.org/docs/server/deleting-streams-and-events/index.html
 */
final private[sec] case class MetaState(
  maxAge: Option[MaxAge],
  maxCount: Option[MaxCount],
  truncateBefore: Option[StreamPosition.Exact],
  cacheControl: Option[CacheControl],
  acl: Option[StreamAcl]
)

private[sec] object MetaState {

  val empty: MetaState = MetaState(None, None, None, None, None)

  ///

  implicit private[sec] val codecForMetaState: Codec.AsObject[MetaState] =
    new Codec.AsObject[MetaState] {

      import Decoder.{decodeInt => di, decodeLong => dl}
      import Encoder.{encodeInt => ei, encodeLong => el}

      final private val cfd: Codec[FiniteDuration] =
        Codec.from(dl.map(FiniteDuration(_, SECONDS)), el.contramap(_.toSeconds))

      implicit val codecForMaxAge: Codec[MaxAge] =
        Codec.from(cfd.emap(MaxAge(_)), cfd.contramap(_.value))

      implicit val codecForMaxCount: Codec[MaxCount] =
        Codec.from(di.emap(MaxCount(_)), ei.contramap(_.value))

      implicit val codecForCacheControl: Codec[CacheControl] =
        Codec.from(cfd.emap(CacheControl(_)), cfd.contramap(_.value))

      implicit val codecForStreamPositionExact: Codec[StreamPosition.Exact] =
        Codec.from(dl.map(StreamPosition.exact), el.contramap(_.value))

      //

      def encodeObject(ms: MetaState): JsonObject = {

        val data = Map(
          metadataKeys.MaxAge         -> ms.maxAge.asJson,
          metadataKeys.TruncateBefore -> ms.truncateBefore.asJson,
          metadataKeys.MaxCount       -> ms.maxCount.asJson,
          metadataKeys.Acl            -> ms.acl.asJson,
          metadataKeys.CacheControl   -> ms.cacheControl.asJson
        )

        JsonObject.fromMap(data).mapValues(_.dropNullValues)
      }

      def apply(c: HCursor): Result[MetaState] =
        for {

          maxAge         <- c.get[Option[MaxAge]](metadataKeys.MaxAge)
          truncateBefore <- c.get[Option[StreamPosition.Exact]](metadataKeys.TruncateBefore)
          maxCount       <- c.get[Option[MaxCount]](metadataKeys.MaxCount)
          acl            <- c.get[Option[StreamAcl]](metadataKeys.Acl)
          cacheControl   <- c.get[Option[CacheControl]](metadataKeys.CacheControl)

        } yield MetaState(maxAge, maxCount, truncateBefore, cacheControl, acl)

    }

  private[sec] object metadataKeys {

    final val MaxAge: String         = "$maxAge"
    final val MaxCount: String       = "$maxCount"
    final val TruncateBefore: String = "$tb"
    final val CacheControl: String   = "$cacheControl"
    final val Acl: String            = "$acl"

  }

  implicit val showForMetaState: Show[MetaState] = Show.show[MetaState] { ss =>
    s"""
       |MetaState:
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
 */
final case class StreamAcl(
  readRoles: Set[String],
  writeRoles: Set[String],
  deleteRoles: Set[String],
  metaReadRoles: Set[String],
  metaWriteRoles: Set[String]
)

object StreamAcl {

  final val empty: StreamAcl = StreamAcl(Set.empty, Set.empty, Set.empty, Set.empty, Set.empty)

  implicit final class StreamAclOps(val sa: StreamAcl) extends AnyVal {

    def withReadRoles(value: Set[String]): StreamAcl      = sa.copy(readRoles = value)
    def withWriteRoles(value: Set[String]): StreamAcl     = sa.copy(writeRoles = value)
    def withMetaReadRoles(value: Set[String]): StreamAcl  = sa.copy(metaReadRoles = value)
    def withMetaWriteRoles(value: Set[String]): StreamAcl = sa.copy(metaWriteRoles = value)
    def withDeleteRoles(value: Set[String]): StreamAcl    = sa.copy(deleteRoles = value)

  }

  ///

  implicit private[sec] val codecForStreamAcl: Codec.AsObject[StreamAcl] = new Codec.AsObject[StreamAcl] {

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
