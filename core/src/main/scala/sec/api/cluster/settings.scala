package sec
package api
package cluster

import scala.concurrent.duration._
import cats.implicits._
import retries._

final private[sec] case class ClusterSettings(
  maxDiscoverAttempts: Option[Int],
  retryDelay: FiniteDuration,
  retryMaxDelay: FiniteDuration,
  retryBackoffFactor: Double,
  readTimeout: FiniteDuration,
  notificationInterval: FiniteDuration,
  preference: NodePreference
)

private[sec] object ClusterSettings {

  val default: ClusterSettings = ClusterSettings(
    maxDiscoverAttempts  = None,
    retryDelay           = 100.millis,
    retryMaxDelay        = 2.seconds,
    retryBackoffFactor   = 1.25,
    readTimeout          = 5.seconds,
    notificationInterval = 100.millis,
    preference           = NodePreference.Leader
  )

  implicit final class ClusterSettingsOps(val cs: ClusterSettings) extends AnyVal {

    def withMaxDiscoverAttempts(max: Option[Int]): ClusterSettings          = cs.copy(maxDiscoverAttempts = max)
    def withRetryDelay(delay: FiniteDuration): ClusterSettings              = cs.copy(retryDelay = delay)
    def withRetryMaxDelay(maxDelay: FiniteDuration): ClusterSettings        = cs.copy(retryMaxDelay = maxDelay)
    def withRetryBackoffFactor(factor: Double): ClusterSettings             = cs.copy(retryBackoffFactor = factor)
    def withReadTimeout(timeout: FiniteDuration): ClusterSettings           = cs.copy(readTimeout = timeout)
    def withNotificationInterval(interval: FiniteDuration): ClusterSettings = cs.copy(notificationInterval = interval)
    def withNodePreference(np: NodePreference): ClusterSettings             = cs.copy(preference = np)

    def retryConfig: RetryConfig = RetryConfig(
      cs.retryDelay,
      cs.retryMaxDelay,
      cs.retryBackoffFactor,
      cs.maxDiscoverAttempts.getOrElse(Int.MaxValue),
      cs.readTimeout.some
    )
  }

}
