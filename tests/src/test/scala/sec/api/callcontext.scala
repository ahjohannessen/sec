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

class UserCredentialsSpec extends SecSuite {

  test("UserCredentials.apply") {

    assertEquals(UserCredentials("hello", "world"), Right(UserCredentials.unsafe("hello", "world")))

    assertEquals(UserCredentials("", "world"), Left(InvalidInput("username is empty")))
    assertEquals(UserCredentials("hello", ""), Left(InvalidInput("password is empty")))

    assertEquals(UserCredentials("hell:o", "world"), Left(InvalidInput("username cannot contain characters [':']")))
    assertEquals(UserCredentials("hello", "worl:d"), Left(InvalidInput("password cannot contain characters [':']")))

    assertEquals(
      UserCredentials("hello", "world").map(_.toString),
      Right("UserCredentials(username = hello, password = 🤐)")
    )

  }
}
