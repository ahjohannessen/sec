package sec
package api
package cluster

import scala.concurrent.duration._

final private[sec] case class ClusterSettings(
  maxDiscoverAttempts: Option[Int],
  retryDelay: FiniteDuration,
  retryStrategy: RetryStrategy,
  readTimeout: FiniteDuration,
  notificationInterval: FiniteDuration,
  preference: NodePreference
)

private[sec] object ClusterSettings {

  val default: ClusterSettings = ClusterSettings(
    maxDiscoverAttempts  = None,
    retryDelay           = 200.millis,
    retryStrategy        = RetryStrategy.Identity,
    readTimeout          = 5.seconds,
    notificationInterval = 100.millis,
    preference           = NodePreference.Leader
  )

  implicit final class SettingsOps(val s: ClusterSettings) extends AnyVal {
    def withMaxDiscoverAttempts(max: Option[Int]): ClusterSettings          = s.copy(maxDiscoverAttempts = max)
    def withRetryDelay(delay: FiniteDuration): ClusterSettings              = s.copy(retryDelay = delay)
    def withReadTimeout(timeout: FiniteDuration): ClusterSettings           = s.copy(readTimeout = timeout)
    def withNotificationInterval(interval: FiniteDuration): ClusterSettings = s.copy(notificationInterval = interval)
    def withNodePreference(np: NodePreference): ClusterSettings             = s.copy(preference = np)
  }

}
