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

  case object PersistentSubscriptionConfig extends SystemId
  case object All                          extends SystemId
  case object Settings                     extends SystemId
  case object Stats                        extends SystemId
  case object Streams                      extends SystemId
  case object UsersPasswordNotifications   extends SystemId

  sealed abstract case class System(name: String) extends SystemId
  sealed abstract case class Normal(name: String) extends NormalId

  def apply[F[_]](name: String)(implicit F: ApplicativeError[F, Throwable]): F[Id] =
    from(name).leftMap(StreamIdError).liftTo[F]

  def from(name: String): Attempt[Id] = {
    guardNonEmptyName(name) >>= guardNotStartWith(metadataPrefix) >>= { n =>
      if (n.startsWith(systemPrefix)) system(n.substring(1)) else normal(n)
    }
  }

  final case class StreamIdError(msg: String) extends RuntimeException(msg)

  ///

  private val guardNonEmptyName: String => Attempt[String] = guardNonEmpty("name")
  private def guardNotStartWith(prefix: String): String => Attempt[String] =
    n => Either.cond(!n.startsWith(prefix), n, s"name must not start with $prefix, but is $n")

  private[sec] def normal(name: String): Attempt[Normal] =
    (guardNonEmptyName(name) >>= guardNotStartWith(systemPrefix)).map(new Normal(_) {})

  private[sec] def system(name: String): Attempt[System] =
    (guardNonEmptyName(name) >>= guardNotStartWith(systemPrefix)).map(new System(_) {})

  ///

  private[sec] val streamIdToString: StreamId => String = {
    case id: Id     => idToString(id)
    case MetaId(id) => s"$metadataPrefix${idToString(id)}"
  }

  private[sec] val stringToStreamId: String => Attempt[StreamId] = {
    case id if id.startsWith(metadataPrefix) => stringToId(id.substring(2)).map(MetaId)
    case id                                  => stringToId(id)
  }

  private[sec] val idToString: Id => String = {
    case PersistentSubscriptionConfig => systemStreams.PersistentSubscriptionConfig
    case All                          => systemStreams.All
    case Settings                     => systemStreams.Settings
    case Stats                        => systemStreams.Stats
    case Streams                      => systemStreams.Streams
    case UsersPasswordNotifications   => systemStreams.UsersPasswordNotifications
    case System(n)                    => s"$systemPrefix$n"
    case Normal(n)                    => n
  }

  private[sec] val stringToId: String => Attempt[Id] = {
    case systemStreams.PersistentSubscriptionConfig => PersistentSubscriptionConfig.asRight
    case systemStreams.All                          => All.asRight
    case systemStreams.Settings                     => Settings.asRight
    case systemStreams.Streams                      => Streams.asRight
    case systemStreams.UsersPasswordNotifications   => UsersPasswordNotifications.asRight
    case sid if sid.startsWith(systemPrefix)        => system(sid.substring(1))
    case sid                                        => normal(sid)
  }

  private[sec] val systemPrefix: String   = "$"
  private[sec] val metadataPrefix: String = "$$"

  private object systemStreams {

    final val PersistentSubscriptionConfig       = "$persistentSubscriptionConfig"
    final val All: String                        = "$all"
    final val Settings: String                   = "$settings"
    final val Stats: String                      = "$stats"
    final val Scavenges: String                  = "$scavenges"
    final val Streams: String                    = "$streams"
    final val UsersPasswordNotifications: String = "$users-password-notifications"

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
