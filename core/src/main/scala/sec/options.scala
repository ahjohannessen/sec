package sec

import cats.implicits._
import api._

//======================================================================================================================

final case class Options(
  connectionName: String,
  nodePreference: NodePreference,
  defaultCreds: Option[UserCredentials]
)

object Options {
  val default = Options("sec", NodePreference.Leader, UserCredentials.unsafe("admin", "changeit").some)
}

//======================================================================================================================

//======================================================================================================================

sealed trait NodePreference
object NodePreference {
  case object Leader          extends NodePreference
  case object Follower        extends NodePreference
  case object Random          extends NodePreference
  case object ReadOnlyReplica extends NodePreference

  implicit final class NodePreferenceOps(val np: NodePreference) extends AnyVal {

    private def fold[A](leader: => A, follower: => A, random: => A, readOnlyReplica: => A): A =
      np match {
        case Leader          => leader
        case Follower        => follower
        case Random          => random
        case ReadOnlyReplica => readOnlyReplica
      }

    def isLeader: Boolean = fold(true, false, false, false)
  }

}
