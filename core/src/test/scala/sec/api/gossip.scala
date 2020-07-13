package sec
package api

import sec.api.Gossip._

class GossipITest extends ITest {

  sequential

  "Gossip" >> {

    "read" >> {
      gossip
        .read(None)
        .map(_.members.headOption should beLike {
          case Some(MemberInfo(_, _, VNodeState.Leader, true, Endpoint("127.0.0.1", 2113))) => ok
        })
    }

  }
}
