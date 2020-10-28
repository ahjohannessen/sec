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

import org.specs2.mutable.Specification

class UserCredentialsSpec extends Specification {

  "UserCredentials.apply" >> {
    UserCredentials("hello", "world") should beRight(UserCredentials.unsafe("hello", "world"))

    UserCredentials("", "world") should beLeft(InvalidInput("username is empty"))
    UserCredentials("hello", "") should beLeft(InvalidInput("password is empty"))

    UserCredentials("hell:o", "world") should beLeft(InvalidInput("username cannot contain characters [':']"))
    UserCredentials("hello", "worl:d") should beLeft(InvalidInput("password cannot contain characters [':']"))

    UserCredentials("hello", "world").map(_.toString) should beRight("UserCredentials(username = hello, password = 🤐)")
  }
}
