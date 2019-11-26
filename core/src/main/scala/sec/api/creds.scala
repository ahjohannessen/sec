package sec
package api

import cats.implicits._

//======================================================================================================================

sealed abstract case class UserCredentials(username: String, password: String) {
  override def toString = s"UserCredentials(username = $username, password = 🤐)"
}

object UserCredentials {

  private[sec] def unsafe(username: String, password: String): UserCredentials =
    new UserCredentials(username, password) {}

  def apply(username: String, password: String): Attempt[UserCredentials] = {

    // TODO: Consult ES devs about valid chars
    def validate(value: String, name: String) = {

      def nonEmpty(v: String): Attempt[String] =
        Option(v).filter(_.isEmpty).toLeft(s"$name is empty")

      def validChars(v: String): Attempt[String] =
        Option.unless(v.contains(':'))(v).toLeft(s"$name cannot contain invalid characters [':']")

      nonEmpty(value) >>= validChars
    }

    (validate(password, "password"), validate(username, "username")).mapN(UserCredentials.unsafe)
  }
}

//======================================================================================================================
