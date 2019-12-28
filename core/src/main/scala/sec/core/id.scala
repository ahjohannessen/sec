package sec
package core

import cats.{ApplicativeError, Eq, Show}
import cats.implicits._

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

  def apply[F[_]](name: String)(implicit F: ApplicativeError[F, Throwable]): F[Id] =
    from(name).leftMap(StreamIdError).liftTo[F]

  def from(name: String): Attempt[Id] = {
    guardNonEmptyName(name) >>= guardNotStartsWith(metadataPrefix) >>= { n =>
      if (n.startsWith(systemPrefix)) system(n.substring(systemPrefixLength)) else normal(n)
    }
  }

  final case class StreamIdError(msg: String) extends RuntimeException(msg)

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

  private[sec] final val systemPrefix: String      = "$"
  private[sec] final val systemPrefixLength: Int   = systemPrefix.length
  private[sec] final val metadataPrefix: String    = "$$"
  private[sec] final val metadataPrefixLength: Int = metadataPrefix.length

  private object systemStreams {
    final val All: String       = "$all"
    final val Settings: String  = "$settings"
    final val Stats: String     = "$stats"
    final val Scavenges: String = "$scavenges"
    final val Streams: String   = "$streams"
  }

  ///

  implicit final class StreamIdOps(val sid: StreamId) extends AnyVal {
    def stringValue: String = streamIdToString(sid)
  }

  implicit final class IdOps(val id: Id) extends AnyVal {
    def meta: MetaId = MetaId(id)
  }

  implicit val eqForStreamId: Eq[StreamId]     = Eq.fromUniversalEquals[StreamId]
  implicit val showForStreamId: Show[StreamId] = Show.show[StreamId](_.stringValue)

}

//======================================================================================================================
