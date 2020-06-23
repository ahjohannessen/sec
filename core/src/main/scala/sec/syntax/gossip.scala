package sec
package syntax

import sec.api._

final class GossipSyntax[F[_]](val s: Gossip[F]) extends AnyVal {

  def read: F[Gossip.ClusterInfo] = s.read(None)

}
