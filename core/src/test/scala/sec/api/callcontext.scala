package sec
package api

import org.specs2._
import org.specs2.mutable.Specification

class UserCredentialsSpec extends Specification {

  "UserCredentials.apply" >> {
    UserCredentials("hello", "world") should beRight(UserCredentials.unsafe("hello", "world"))

    UserCredentials("", "world") should beLeft("username is empty")
    UserCredentials("hello", "") should beLeft("password is empty")

    UserCredentials("hell:o", "world") should beLeft("username cannot contain characters [':']")
    UserCredentials("hello", "worl:d") should beLeft("password cannot contain characters [':']")

    UserCredentials("hello", "world").map(_.toString) should beRight("UserCredentials(username = hello, password = 🤐)")
  }
}
