/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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

import scala.concurrent.duration.*
import cats.{ApplicativeThrow, Endo}
import cats.syntax.all.*
import io.circe.Decoder.Result
import io.circe.*
import io.circe.syntax.*

//======================================================================================================================

final private[sec] case class StreamMetadata(
  state: MetaState,
  custom: Option[JsonObject]
)

private[sec] object StreamMetadata:

  final val empty: StreamMetadata             = StreamMetadata(MetaState.empty)
  def apply(state: MetaState): StreamMetadata = StreamMetadata(state, None)

  extension (sm: StreamMetadata)

    def truncateBefore: Option[StreamPosition.Exact] = sm.state.truncateBefore
    def maxAge: Option[MaxAge]                       = sm.state.maxAge
    def maxCount: Option[MaxCount]                   = sm.state.maxCount
    def cacheControl: Option[CacheControl]           = sm.state.cacheControl
    def acl: Option[StreamAcl]                       = sm.state.acl

    //

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
    def withCustom(value: (String, Json)*): StreamMetadata = sm.withCustom(JsonObject.fromMap(Map.from(value)))

    //

    def getCustom[F[_]: ApplicativeThrow, T: Decoder]: F[Option[T]] =
      sm.custom.traverse(jo => Decoder[T].apply(Json.fromJsonObject(jo).hcursor).liftTo[F])

    def setCustom[T: Encoder.AsObject](custom: T): StreamMetadata =
      sm.withCustom(Encoder.AsObject[T].encodeObject(custom))

    def modifyCustom[F[_]: ApplicativeThrow, T: Codec.AsObject](fn: Endo[Option[T]]): F[StreamMetadata] =
      getCustom[F, T].map(c => sm.setCustom(fn(c).map(Encoder.AsObject[T].encodeObject)))

  //

  import MetaState.metadataKeys

  final val reservedKeys: Set[String] = Set(
    metadataKeys.MaxAge,
    metadataKeys.TruncateBefore,
    metadataKeys.MaxCount,
    metadataKeys.Acl,
    metadataKeys.CacheControl
  )

  given Codec.AsObject[StreamMetadata] = new Codec.AsObject[StreamMetadata]:

    val encodeSS: MetaState => JsonObject         = Encoder.AsObject[MetaState].encodeObject(_)
    val decodeSS: JsonObject => Result[MetaState] = jo => Decoder[MetaState].apply(jo.asJson.hcursor)

    def encodeObject(sm: StreamMetadata): JsonObject = sm.custom match
      case Some(c) if c.nonEmpty => encodeSS(sm.state).toList.foldLeft(c) { case (i, (k, v)) => i.add(k, v) }
      case _                     => encodeSS(sm.state)

    def apply(c: HCursor): Result[StreamMetadata] =
      Decoder.decodeJsonObject(c).map { both =>
        val (ours, theirs) = both.toList.partition(kv => reservedKeys.contains(kv._1))
        (JsonObject.fromIterable(ours), JsonObject.fromIterable(theirs))
      } >>= { case (s, c) =>
        decodeSS(s).map(StreamMetadata(_, Option.when(c.nonEmpty)(c)))
      }

//======================================================================================================================

/** The maximum age of events in the stream. Events older than this will be automatically removed.
  */
sealed abstract case class MaxAge(value: FiniteDuration)
object MaxAge:

  /** The maximum age of events in the stream. Events older than this will be automatically removed.
    *
    * @param maxAge
    *   must be greater than or equal to 1 second.
    */
  def apply(maxAge: FiniteDuration): Either[InvalidInput, MaxAge] =
    if (maxAge < 1.second) InvalidInput(s"maxAge must be >= 1 second, it was $maxAge.").asLeft
    else new MaxAge(maxAge) {}.asRight

  private[sec] def render(ma: MaxAge): String = ma.value.toString()

/** The maximum count of events in the stream. When the stream has more than max count then the oldest will be removed.
  */
sealed abstract case class MaxCount(value: Int)
object MaxCount:

  /** @param maxCount
    *   must be greater than or equal to 1.
    */
  def apply(maxCount: Int): Either[InvalidInput, MaxCount] =
    if (maxCount < 1) InvalidInput(s"max count must be >= 1, it was $maxCount.").asLeft
    else new MaxCount(maxCount) {}.asRight

  private[sec] def render(mc: MaxCount): String =
    s"${mc.value} event${if (mc.value == 1) "" else "s"}"

/** Used for the ATOM API of KurrentDB. The head of a feed in the ATOM API is not cacheable. This value allows you to
  * specify a period of time you want it to be cacheable. Low numbers are best here, e.g. 30-60 seconds, and introducing
  * values here will introduce latency over the ATOM protocol if caching is occuring.
  */
sealed abstract case class CacheControl(value: FiniteDuration)
object CacheControl:

  /** @param cacheControl
    *   must be greater than or equal to 1 second.
    */
  def apply(cacheControl: FiniteDuration): Either[InvalidInput, CacheControl] =
    if (cacheControl < 1.second) InvalidInput(s"cache control must be >= 1, it was $cacheControl.").asLeft
    else new CacheControl(cacheControl) {}.asRight

  private[sec] def render(cc: CacheControl): String = cc.value.toString()

//======================================================================================================================

/** @param maxAge
  *   The maximum age of events in the stream. Items older than this will be automatically removed.
  *
  * @param maxCount
  *   The maximum count of events in the stream. When you have more than count the oldest will be removed.
  *
  * @param truncateBefore
  *   When set says that events with a stream position less than the truncated before value should be removed.
  *
  * @param cacheControl
  *   The head of a feed in the atom api is not cacheable. This allows you to specify a period of time you want it to be
  *   cacheable. Low numbers are best here (say 30-60 seconds) and introducing values here will introduce latency over
  *   the atom protocol if caching is occuring.
  *
  * @param acl
  *   The access control list for this stream.
  *
  * @note
  *   More details are here https://docs.kurrent.io/server/v25.0/features/streams.html#deleting-streams-and-events
  */
final private[sec] case class MetaState(
  maxAge: Option[MaxAge],
  maxCount: Option[MaxCount],
  truncateBefore: Option[StreamPosition.Exact],
  cacheControl: Option[CacheControl],
  acl: Option[StreamAcl]
)

private[sec] object MetaState:

  val empty: MetaState = MetaState(None, None, None, None, None)

  //

  private[sec] given Codec.AsObject[MetaState] =
    new Codec.AsObject[MetaState]:

      import Decoder.{decodeInt => di, decodeLong => dl}
      import Encoder.{encodeInt => ei, encodeLong => el}

      final private val cfd: Codec[FiniteDuration] =
        Codec.from(dl.map(FiniteDuration(_, SECONDS)), el.contramap(_.toSeconds))

      given Codec[MaxAge] =
        Codec.from(cfd.emap(MaxAge(_).leftMap(_.msg)), cfd.contramap(_.value))

      given Codec[MaxCount] =
        Codec.from(di.emap(MaxCount(_).leftMap(_.msg)), ei.contramap(_.value))

      given Codec[CacheControl] =
        Codec.from(cfd.emap(CacheControl(_).leftMap(_.msg)), cfd.contramap(_.value))

      given Codec[StreamPosition.Exact] =
        Codec.from(dl.map(StreamPosition(_)), el.contramap(_.value.toLong))

      //

      def encodeObject(ms: MetaState): JsonObject =

        val data = Map(
          metadataKeys.MaxAge         -> ms.maxAge.asJson,
          metadataKeys.TruncateBefore -> ms.truncateBefore.asJson,
          metadataKeys.MaxCount       -> ms.maxCount.asJson,
          metadataKeys.Acl            -> ms.acl.asJson,
          metadataKeys.CacheControl   -> ms.cacheControl.asJson
        )

        JsonObject.fromMap(data).mapValues(_.dropNullValues)

      def apply(c: HCursor): Result[MetaState] =
        for
          maxAge         <- c.get[Option[MaxAge]](metadataKeys.MaxAge)
          truncateBefore <- c.get[Option[StreamPosition.Exact]](metadataKeys.TruncateBefore)
          maxCount       <- c.get[Option[MaxCount]](metadataKeys.MaxCount)
          acl            <- c.get[Option[StreamAcl]](metadataKeys.Acl)
          cacheControl   <- c.get[Option[CacheControl]](metadataKeys.CacheControl)
        yield MetaState(maxAge, maxCount, truncateBefore, cacheControl, acl)

  private[sec] object metadataKeys:

    final val MaxAge: String         = "$maxAge"
    final val MaxCount: String       = "$maxCount"
    final val TruncateBefore: String = "$tb"
    final val CacheControl: String   = "$cacheControl"
    final val Acl: String            = "$acl"

  def renderMetaState(ms: MetaState): String =
    s"""
       |MetaState:
       |  max-age         = ${ms.maxAge.map(MaxAge.render).getOrElse("n/a")}
       |  max-count       = ${ms.maxCount.map(MaxCount.render).getOrElse("n/a")}
       |  cache-control   = ${ms.cacheControl.map(CacheControl.render).getOrElse("n/a")}
       |  truncate-before = ${ms.truncateBefore.map(e => s"${e.value.render}").getOrElse("n/a")}
       |  access-list     = ${ms.acl.map(StreamAcl.render).getOrElse("n/a")}
       |""".stripMargin

  extension (ms: MetaState)
    def render: String =
      MetaState.renderMetaState(ms)

end MetaState

//======================================================================================================================

/** Access Control List for a stream.
  *
  * @param readRoles
  *   Roles and users permitted to read the stream.
  * @param writeRoles
  *   Roles and users permitted to write to the stream.
  * @param deleteRoles
  *   Roles and users permitted to delete the stream.
  * @param metaReadRoles
  *   Roles and users permitted to read stream metadata.
  * @param metaWriteRoles
  *   Roles and users permitted to write stream metadata.
  */
final case class StreamAcl(
  readRoles: Set[String],
  writeRoles: Set[String],
  deleteRoles: Set[String],
  metaReadRoles: Set[String],
  metaWriteRoles: Set[String]
)

object StreamAcl:

  final val empty: StreamAcl =
    StreamAcl(Set.empty, Set.empty, Set.empty, Set.empty, Set.empty)

  extension (sa: StreamAcl)

    def withReadRoles(value: Set[String]): StreamAcl      = sa.copy(readRoles = value)
    def withWriteRoles(value: Set[String]): StreamAcl     = sa.copy(writeRoles = value)
    def withMetaReadRoles(value: Set[String]): StreamAcl  = sa.copy(metaReadRoles = value)
    def withMetaWriteRoles(value: Set[String]): StreamAcl = sa.copy(metaWriteRoles = value)
    def withDeleteRoles(value: Set[String]): StreamAcl    = sa.copy(deleteRoles = value)
    def render: String                                    = StreamAcl.renderStreamAcl(sa)

  //

  private[sec] given Codec.AsObject[StreamAcl] = new Codec.AsObject[StreamAcl]:

    def encodeObject(a: StreamAcl): JsonObject =

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

    def apply(c: HCursor): Result[StreamAcl] =

      def get(k: String): Result[Set[String]] =
        c.getOrElse[Set[String]](k)(Set.empty)(Decoder[Set[String]].or(Decoder[String].map(Set(_))))

      (get(aclKeys.Read), get(aclKeys.Write), get(aclKeys.Delete), get(aclKeys.MetaRead), get(aclKeys.MetaWrite))
        .mapN(StreamAcl.apply)

  private[sec] object aclKeys:

    final val Read: String            = "$r"
    final val Write: String           = "$w"
    final val Delete: String          = "$d"
    final val MetaRead: String        = "$mr"
    final val MetaWrite: String       = "$mw"
    final val UserStreamAcl: String   = "$userStreamAcl"
    final val SystemStreamAcl: String = "$systemStreamAcl"

  def renderStreamAcl(sa: StreamAcl): String =

    def show(label: String, roles: Set[String]): String =
      s"$label: ${roles.mkString("[", ", ", "]")}"

    val r  = show("read", sa.readRoles)
    val w  = show("write", sa.writeRoles)
    val d  = show("delete", sa.deleteRoles)
    val mr = show("meta-read", sa.metaReadRoles)
    val mw = show("meta-write", sa.metaWriteRoles)

    s"$r, $w, $d, $mr, $mw"

end StreamAcl
