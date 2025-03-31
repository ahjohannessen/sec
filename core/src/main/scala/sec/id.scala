/*
 * Copyright 2020 Scala Event Sourcing client for KurrentDB
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

import cats.Eq
import cats.syntax.all.*
import sec.utilities.{guardNonEmpty, guardNotStartsWith}

//======================================================================================================================

/** Stream identifier for streams in KurrentDB. There are three variants:
  *
  *   - [[StreamId.System]] identifier used for reserverd internal system streams.
  *   - [[StreamId.Normal]] identifier used by users.
  *   - [[StreamId.MetaId]] identifier used for metadata streams of [[StreamId.System]] streams or [[StreamId.Normal]]
  *     streams.
  */
sealed trait StreamId
object StreamId:

  sealed trait Id extends StreamId
  final case class MetaId(id: Id) extends StreamId

  sealed abstract case class System(name: String) extends Id
  private[sec] object System:
    def unsafe(name: String): System = new System(name) {}

  sealed abstract case class Normal(name: String) extends Id
  private[sec] object Normal:
    def unsafe(name: String): Normal = new Normal(name) {}

  //

  val All: System       = System.unsafe("all")
  val Settings: System  = System.unsafe("settings")
  val Stats: System     = System.unsafe("stats")
  val Scavenges: System = System.unsafe("scavenges")
  val Streams: System   = System.unsafe("streams")

  /** @param name
    *   Constructs a stream identifier for a stream. Provided value is validated for non-empty and not starting with the
    *   system reserved metadata prefix `$$`.
    */
  def apply(name: String): Either[InvalidInput, Id] =
    (guardNonEmptyName(name) >>= guardNotStartsWith(metadataPrefix) >>= stringToId).leftMap(InvalidInput(_))

  //

  private[sec] val guardNonEmptyName: String => Attempt[String] = guardNonEmpty("name")

  private[sec] def normal(name: String): Attempt[Normal] =
    (guardNonEmptyName(name) >>= guardNotStartsWith(systemPrefix)).map(Normal.unsafe)

  private[sec] def system(name: String): Attempt[System] =
    (guardNonEmptyName(name) >>= guardNotStartsWith(systemPrefix)).map(System.unsafe)

  private[sec] val streamIdToString: StreamId => String =
    case id: Id     => idToString(id)
    case MetaId(id) => s"$metadataPrefix${idToString(id)}"

  private[sec] val stringToStreamId: String => Attempt[StreamId] =
    case id if id.startsWith(metadataPrefix) => stringToId(id.substring(metadataPrefixLength)).map(MetaId(_))
    case id                                  => stringToId(id)

  private[sec] val idToString: Id => String =
    case System(n) => s"$systemPrefix$n"
    case Normal(n) => n

  private[sec] val stringToId: String => Attempt[Id] =
    case sid if sid.startsWith(systemPrefix) => system(sid.substring(systemPrefixLength))
    case sid                                 => normal(sid)

  private[sec] val systemPrefix: String      = "$"
  private[sec] val systemPrefixLength: Int   = systemPrefix.length
  private[sec] val metadataPrefix: String    = "$$"
  private[sec] val metadataPrefixLength: Int = metadataPrefix.length

  //

  extension (sid: StreamId)

    def fold[A](nfn: Normal => A, sfn: System => A, mfn: MetaId => A): A = sid match
      case n: Normal => nfn(n)
      case s: System => sfn(s)
      case m: MetaId => mfn(m)

    def stringValue: String     = streamIdToString(sid)
    def render: String          = stringValue
    def isNormal: Boolean       = fold(_ => true, _ => false, _ => false)
    def isSystemOrMeta: Boolean = fold(_ => false, _ => true, _ => true)

  extension (id: Id) def metaId: MetaId = MetaId(id)

  given Eq[StreamId] = Eq.fromUniversalEquals[StreamId]

end StreamId

//======================================================================================================================
