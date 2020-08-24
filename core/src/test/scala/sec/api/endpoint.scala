package sec
package api

import cats.implicits._
import cats.kernel.laws.discipline._
import org.specs2.mutable.Specification
import org.scalacheck._
import org.typelevel.discipline.specs2.mutable.Discipline
import Arbitraries._

class EndpointSpec extends Specification with Discipline {

  "Endpoint" >> {

    "order" >> {
      implicit val cogen: Cogen[Endpoint] = Cogen[String].contramap[Endpoint](_.toString)
      checkAll("Endpoint", OrderTests[Endpoint].order)
    }

    "show" >> {
      Endpoint("127.0.0.1", 2113).show shouldEqual "127.0.0.1:2113"
    }
  }

}
