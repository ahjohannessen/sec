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
    maxDiscoverAttempts  = Int.MaxValue,
    retryDelay           = 250.millis,
    readTimeout          = 2.seconds,
    notificationInterval = 500.millis,
    preference           = NodePreference.Leader
  )

}
