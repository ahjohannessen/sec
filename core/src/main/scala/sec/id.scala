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

import cats.syntax.all._
import cats.{Eq, Show}
import sec.utilities.{guardNonEmpty, guardNotStartsWith}

//======================================================================================================================

sealed trait StreamId
object StreamId {

  sealed trait Id       extends StreamId
  sealed trait NormalId extends Id
  sealed trait SystemId extends Id

  final case class MetaId(id: Id) extends StreamId

  case object All       extends SystemId
  case object Settings  extends SystemId
  case object Stats     extends SystemId
  case object Scavenges extends SystemId
  case object Streams   extends SystemId

  sealed abstract case class System(name: String) extends SystemId
  sealed abstract case class Normal(name: String) extends NormalId

  def apply(name: String): Either[InvalidInput, Id] =
    (guardNonEmptyName(name) >>= guardNotStartsWith(metadataPrefix) >>= stringToId).leftMap(InvalidInput)

  ///

  private[sec] val guardNonEmptyName: String => Attempt[String] = guardNonEmpty("name")

  private[sec] def normal(name: String): Attempt[Normal] =
    (guardNonEmptyName(name) >>= guardNotStartsWith(systemPrefix)).map(new Normal(_) {})

  private[sec] def system(name: String): Attempt[System] =
    (guardNonEmptyName(name) >>= guardNotStartsWith(systemPrefix)).map(new System(_) {})

  ///

  private[sec] val streamIdToString: StreamId => String = {
    case id: Id     => idToString(id)
    case MetaId(id) => s"$metadataPrefix${idToString(id)}"
  }

  private[sec] val stringToStreamId: String => Attempt[StreamId] = {
    case id if id.startsWith(metadataPrefix) => stringToId(id.substring(metadataPrefixLength)).map(MetaId)
    case id                                  => stringToId(id)
  }

  private[sec] val idToString: Id => String = {
    case All       => systemStreams.All
    case Settings  => systemStreams.Settings
    case Stats     => systemStreams.Stats
    case Scavenges => systemStreams.Scavenges
    case Streams   => systemStreams.Streams
    case System(n) => s"$systemPrefix$n"
    case Normal(n) => n
  }

  private[sec] val stringToId: String => Attempt[Id] = {
    case systemStreams.All                   => All.asRight
    case systemStreams.Settings              => Settings.asRight
    case systemStreams.Stats                 => Stats.asRight
    case systemStreams.Scavenges             => Scavenges.asRight
    case systemStreams.Streams               => Streams.asRight
    case sid if sid.startsWith(systemPrefix) => system(sid.substring(systemPrefixLength))
    case sid                                 => normal(sid)
  }

  final private[sec] val systemPrefix: String      = "$"
  final private[sec] val systemPrefixLength: Int   = systemPrefix.length
  final private[sec] val metadataPrefix: String    = "$$"
  final private[sec] val metadataPrefixLength: Int = metadataPrefix.length

  private[sec] object systemStreams {
    final val All: String       = "$all"
    final val Settings: String  = "$settings"
    final val Stats: String     = "$stats"
    final val Scavenges: String = "$scavenges"
    final val Streams: String   = "$streams"
  }

  ///

  implicit final class StreamIdOps(val sid: StreamId) extends AnyVal {

    private[sec] def fold[A](nfn: NormalId => A, sfn: SystemId => A, mfn: MetaId => A): A =
      sid match {
        case n: NormalId => nfn(n)
        case s: SystemId => sfn(s)
        case m: MetaId   => mfn(m)
      }

    def stringValue: String     = streamIdToString(sid)
    def isNormal: Boolean       = fold(_ => true, _ => false, _ => false)
    def isSystemOrMeta: Boolean = fold(_ => false, _ => true, _ => true)

  }

  implicit final class IdOps(val id: Id) extends AnyVal {
    def metaId: MetaId = MetaId(id)
  }

  implicit val eqForStreamId: Eq[StreamId]     = Eq.fromUniversalEquals[StreamId]
  implicit val showForStreamId: Show[StreamId] = Show.show[StreamId](_.stringValue)

}

//======================================================================================================================
