/*
 * Copyright 2020 Scala EventStoreDB Client
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

import cats.syntax.all._

//======================================================================================================================

final private[sec] case class Context(
  connectionName: String,
  userCreds: Option[UserCredentials],
  requiresLeader: Boolean
)

//======================================================================================================================

/**
 * Credentials used for EventStoreDB connections.
 */

sealed abstract case class UserCredentials(username: String, password: String) {
  override def toString = s"UserCredentials(username = $username, password = 🤐)"
}

object UserCredentials {

  private[sec] def unsafe(username: String, password: String): UserCredentials =
    new UserCredentials(username, password) {}

  /**
   * Constructs an instance with provided @param username and @param password. Input is validated
   * for being non-empty and not containing the character `:`.
   */
  def apply(username: String, password: String): Either[InvalidInput, UserCredentials] = {

    def validate(value: String, name: String) = {

      def nonEmpty(v: String): Attempt[String] =
        Option(v).filter(_.nonEmpty).toRight(s"$name is empty")

      def validChars(v: String): Attempt[String] =
        Option.unless(v.contains(':'))(v).toRight(s"$name cannot contain characters [':']")

      nonEmpty(value) >>= validChars
    }

    (validate(username, "username"), validate(password, "password"))
      .mapN(UserCredentials.unsafe)
      .leftMap(InvalidInput(_))
  }
}

//======================================================================================================================
