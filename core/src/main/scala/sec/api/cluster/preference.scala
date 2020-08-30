package sec
package api
package cluster

import cats.Eq
import cats.implicits._

sealed trait NodePreference
object NodePreference {

  case object Leader          extends NodePreference
  case object Follower        extends NodePreference
  case object ReadOnlyReplica extends NodePreference

  implicit val eqForNodePreference: Eq[NodePreference] =
    Eq.fromUniversalEquals[NodePreference]

  implicit final class NodePreferenceOps(val np: NodePreference) extends AnyVal {
    def isLeader: Boolean = np.eqv(Leader)
  }

}
