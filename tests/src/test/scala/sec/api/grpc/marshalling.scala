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
package grpc

import org.specs2._

class MarshallingSpec extends mutable.Specification {

  "NumericAsciiMarshallers" >> {

    /// Happy Path

    IntMarshaller.toAsciiString(Int.MinValue) shouldEqual "-2147483648"
    IntMarshaller.parseAsciiString("-2147483648") shouldEqual Int.MinValue
    IntMarshaller.toAsciiString(0) shouldEqual "0"
    IntMarshaller.parseAsciiString("0") shouldEqual 0
    IntMarshaller.toAsciiString(-0) shouldEqual "0"
    IntMarshaller.parseAsciiString("-0") shouldEqual 0
    IntMarshaller.toAsciiString(Int.MaxValue) shouldEqual "2147483647"
    IntMarshaller.parseAsciiString("2147483647") shouldEqual Int.MaxValue

    LongMarshaller.toAsciiString(Long.MinValue) shouldEqual "-9223372036854775808"
    LongMarshaller.parseAsciiString("-9223372036854775808") shouldEqual Long.MinValue
    LongMarshaller.toAsciiString(0L) shouldEqual "0"
    LongMarshaller.parseAsciiString("0") shouldEqual 0L
    LongMarshaller.toAsciiString(-0L) shouldEqual "0"
    LongMarshaller.parseAsciiString("-0") shouldEqual 0L
    LongMarshaller.toAsciiString(Long.MaxValue) shouldEqual "9223372036854775807"
    LongMarshaller.parseAsciiString("9223372036854775807") shouldEqual Long.MaxValue

    /// Sad Path

    InvalidInput("Blackie Lawless", "WASP").getMessage() shouldEqual "Could not parse Blackie Lawless to WASP"

    IntMarshaller.parseAsciiString("-2147483649") should throwAn(InvalidInput("-2147483649", "Int"))
    IntMarshaller.parseAsciiString("Chuck Norris") should throwAn(InvalidInput("Chuck Norris", "Int"))
    IntMarshaller.parseAsciiString("2147483648") should throwAn(InvalidInput("2147483648", "Int"))

    LongMarshaller.parseAsciiString("-9223372036854775809") should throwAn(InvalidInput("-9223372036854775809", "Long"))
    LongMarshaller.parseAsciiString("Johnny Cash") should throwAn(InvalidInput("Johnny Cash", "Long"))
    LongMarshaller.parseAsciiString("9223372036854775808") should throwAn(InvalidInput("9223372036854775808", "Long"))
  }

  "StringMarshaller" >> {
    StringMarshaller.toAsciiString("Willie Nelson") shouldEqual "Willie Nelson"
    StringMarshaller.parseAsciiString("Waylon Jennings") shouldEqual "Waylon Jennings"
  }

  "UserCredentialsMarshaller" >> {
    UserCredentialsMarshaller.toAsciiString(
      UserCredentials.unsafe("Kris", "Kristofferson")) shouldEqual "Basic S3JpczpLcmlzdG9mZmVyc29u"
    UserCredentialsMarshaller.parseAsciiString(
      "Basic S3JpczpLcmlzdG9mZmVyc29u") shouldEqual UserCredentialsMarshaller.decodingNotSupported
  }

}
