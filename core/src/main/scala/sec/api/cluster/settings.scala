package sec
package api
package cluster

import scala.concurrent.duration._

final private[sec] case class Settings(
  maxDiscoverAttempts: Option[Int],
  retryDelay: FiniteDuration,
  retryStrategy: RetryStrategy,
  readTimeout: FiniteDuration,
  notificationInterval: FiniteDuration,
  preference: NodePreference
)

private[sec] object Settings {

  val default: Settings = Settings(
    maxDiscoverAttempts  = None,
    retryDelay           = 200.millis,
    retryStrategy        = RetryStrategy.Identity,
    readTimeout          = 2.seconds,
    notificationInterval = 100.millis,
    preference           = NodePreference.Leader
  )

}
