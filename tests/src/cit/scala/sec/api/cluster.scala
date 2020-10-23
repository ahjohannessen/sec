/*
 * Copyright 2020 Alex Henning Johannessen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sec
package api

import scala.concurrent.duration._
import cats.implicits._

class ClusterSuite extends CSpec {

  "Cluster" should {

    "be reachable" >> {

      fs2.Stream
        .eval(gossip.read)
        .evalTap(x => log.info(s"Gossip.read: ${x.show}"))
        .metered(150.millis)
        .repeat
        .take(5)
        .compile
        .lastOrError
        .map(_.members)
        .map(m => (m.size mustEqual 3) and (m.forall(_.isAlive) should beTrue))

    }

  }

}
