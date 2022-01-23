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
package grpc

class MarshallingSuite extends SecSuite {

  test("NumericAsciiMarshallers") {

    // Happy Path

    assertEquals(IntMarshaller.toAsciiString(Int.MinValue), "-2147483648")
    assertEquals(IntMarshaller.parseAsciiString("-2147483648"), Int.MinValue)
    assertEquals(IntMarshaller.toAsciiString(0), "0")
    assertEquals(IntMarshaller.parseAsciiString("0"), 0)
    assertEquals(IntMarshaller.toAsciiString(-0), "0")
    assertEquals(IntMarshaller.parseAsciiString("-0"), 0)
    assertEquals(IntMarshaller.toAsciiString(Int.MaxValue), "2147483647")
    assertEquals(IntMarshaller.parseAsciiString("2147483647"), Int.MaxValue)

    assertEquals(LongMarshaller.toAsciiString(Long.MinValue), "-9223372036854775808")
    assertEquals(LongMarshaller.parseAsciiString("-9223372036854775808"), Long.MinValue)
    assertEquals(LongMarshaller.toAsciiString(0L), "0")
    assertEquals(LongMarshaller.parseAsciiString("0"), 0L)
    assertEquals(LongMarshaller.toAsciiString(-0L), "0")
    assertEquals(LongMarshaller.parseAsciiString("-0"), 0L)
    assertEquals(LongMarshaller.toAsciiString(Long.MaxValue), "9223372036854775807")
    assertEquals(LongMarshaller.parseAsciiString("9223372036854775807"), Long.MaxValue)

    // Sad Path

    assertEquals(InvalidInput("Blackie Lawless", "WASP").getMessage(), "Could not parse Blackie Lawless to WASP")

    interceptMessage[InvalidInput]("Could not parse -2147483649 to Int")(
      IntMarshaller.parseAsciiString("-2147483649")
    )

    interceptMessage[InvalidInput]("Could not parse Chuck Norris to Int")(
      IntMarshaller.parseAsciiString("Chuck Norris")
    )

    interceptMessage[InvalidInput]("Could not parse 2147483648 to Int")(
      IntMarshaller.parseAsciiString("2147483648")
    )

    interceptMessage[InvalidInput]("Could not parse -9223372036854775809 to Long")(
      LongMarshaller.parseAsciiString("-9223372036854775809")
    )

    interceptMessage[InvalidInput]("Could not parse Johnny Cash to Long")(
      LongMarshaller.parseAsciiString("Johnny Cash")
    )

    interceptMessage[InvalidInput]("Could not parse 9223372036854775808 to Long")(
      LongMarshaller.parseAsciiString("9223372036854775808")
    )

  }

  test("StringMarshaller") {
    assertEquals(StringMarshaller.toAsciiString("Willie Nelson"), "Willie Nelson")
    assertEquals(StringMarshaller.parseAsciiString("Waylon Jennings"), "Waylon Jennings")
  }

  test("UserCredentialsMarshaller") {

    assertEquals(
      UserCredentialsMarshaller.toAsciiString(UserCredentials.unsafe("Kris", "Kristofferson")),
      "Basic S3JpczpLcmlzdG9mZmVyc29u"
    )

    assertEquals(
      UserCredentialsMarshaller.parseAsciiString("Basic S3JpczpLcmlzdG9mZmVyc29u"),
      UserCredentialsMarshaller.decodingNotSupported
    )
  }

}
