package sec
package cluster

import scala.concurrent.duration._

final case class Settings(
  maxDiscoverAttempts: Int,
  retryDelay: FiniteDuration,
  readTimeout: FiniteDuration,
  notificationInterval: FiniteDuration,
  preference: NodePreference
)

object Settings {

  val default: Settings = Settings(
    maxDiscoverAttempts  = 25,
    retryDelay           = 100.millis,
    readTimeout          = 5.seconds,
    notificationInterval = 100.millis,
    preference           = NodePreference.Leader
  )

}
