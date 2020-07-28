package sec

import scala.concurrent.duration._
import cats.implicits._
import api.cluster.NodePreference
import api.UserCredentials

//======================================================================================================================

// TODO: Redo this wrt. cluster / operation / log / ... settings

final private[sec] case class Options(
  connectionName: String,
  nodePreference: NodePreference,
  defaultCreds: Option[UserCredentials],
  operationOptions: OperationOptions
)

private[sec] object Options {
  val default: Options = Options(
    connectionName   = "sec",
    nodePreference   = NodePreference.Leader,
    defaultCreds     = UserCredentials.unsafe("admin", "changeit").some,
    operationOptions = OperationOptions.default
  )
}

final private[sec] case class OperationOptions(
  retryDelay: FiniteDuration,
  retryStrategy: RetryStrategy,
  retryMaxAttempts: Int
)

private[sec] object OperationOptions {

  val default: OperationOptions = OperationOptions(
    retryDelay       = 200.millis,
    retryStrategy    = RetryStrategy.Identity,
    retryMaxAttempts = 100
  )
}

//======================================================================================================================

sealed private[sec] trait RetryStrategy
private[sec] object RetryStrategy {

  case object Identity    extends RetryStrategy
  case object Exponential extends RetryStrategy

  implicit final class RetryStrategyOps(val s: RetryStrategy) extends AnyVal {
    def nextDelay(d: FiniteDuration): FiniteDuration = s match {
      case Identity    => d
      case Exponential => d * 2
    }
  }
}
