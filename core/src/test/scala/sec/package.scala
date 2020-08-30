package sec

import cats.implicits._
import org.specs2.mutable.Specification
import core.ValidationError

class SecPackageSpec extends Specification {

  "AttemptOps" >> {
    "unsafe" >> {
      "oops".asLeft[Int].unsafe should throwA[IllegalArgumentException]("oops")
      1.asRight[String].unsafe shouldEqual 1
    }

    "orFail" >> {
      "oops".asLeft[Int].orFail[ErrorOr](ValidationError(_)) shouldEqual ValidationError("oops").asLeft
    }
  }

  "BooleanOps" >> {
    "fold" >> {
      true.fold("t", "f") shouldEqual "t"
      false.fold("t", "f") shouldEqual "f"
    }
  }

  "guardNonEmpty" >> {
    guardNonEmpty("x")(null) should beLeft("x cannot be empty")
    guardNonEmpty("x")("") should beLeft("x cannot be empty")
    guardNonEmpty("x")("wohoo") should beRight("wohoo")
  }

  "guardNotStartsWith" >> {
    guardNotStartsWith("$")("$") should beLeft("value must not start with $, but is $")
    guardNotStartsWith("$")("a$") should beRight("a$")
  }

}
