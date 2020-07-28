package sec
package cluster

import scala.concurrent.duration._

final case class Settings(
  maxDiscoverAttempts: Option[Int],
  retryDelay: FiniteDuration,
  readTimeout: FiniteDuration,
  notificationInterval: FiniteDuration,
  preference: NodePreference
)

object Settings {

  val default: Settings = Settings(
    maxDiscoverAttempts  = None,
    retryDelay           = 200.millis,
    readTimeout          = 2.seconds,
    notificationInterval = 100.millis,
    preference           = NodePreference.Leader
  )

}
