/*
 * Copyright 2020 Scala Event Sourcing client for KurrentDB
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

import cats.syntax.all.*
import cats.{Eq, Order}

//======================================================================================================================

/** The expected state that a stream is currently in. There are four variants:
  *
  *   - [[StreamState.NoStream]] the stream does not exist yet.
  *   - [[StreamState.Any]] No expectation of the current stream state.
  *   - [[StreamState.StreamExists]] The stream, or its metadata stream, exists.
  *   - [[StreamPosition.Exact]] The stream exists and its last written stream position is expected to be an exact
  *     value.
  *
  * ==Use Cases==
  *
  * When you write to a stream for the first time you provide [[StreamState.NoStream]]. In order to decide if
  * [[StreamState.NoStream]] is required you can try to read from the stream and if the read operation raises
  * [[sec.api.exceptions.StreamNotFound]] you know that your expectation should be [[StreamState.NoStream]].
  *
  * When you do not have any expectation of the current state of a stream you should use [[StreamState.Any]]. This is,
  * for instance, used when you just wish to append data to a stream regardless of other concurrent operations to the
  * stream.
  *
  * When you require that a stream, or its metadata stream, is present you should use [[StreamState.StreamExists]].
  *
  * When you need to implement optimistic concurrency you use [[StreamPosition.Exact]] and [[StreamState.NoStream]] as
  * your exected stream state. You use [[StreamState.NoStream]] as expected stream state when you append to a stream for
  * the first time, otherwise you use an [[StreamPosition.Exact]] value. A [[sec.api.exceptions.WrongExpectedState]]
  * exception is rasised when the stream exists and has changed in the meantime.
  */
sealed trait StreamState
object StreamState {

  case object NoStream extends StreamState
  case object Any extends StreamState
  case object StreamExists extends StreamState

  given Eq[StreamState] = Eq.fromUniversalEquals

  def renderStreamState(ss: StreamState): String = ss match
    case NoStream                => "NoStream"
    case Any                     => "Any"
    case StreamExists            => "StreamExists"
    case StreamPosition.Exact(v) => s"Exact(${v.render})"

  extension (ss: StreamState)
    def render: String =
      StreamState.renderStreamState(ss)

}

//======================================================================================================================

/** Stream position in an individual stream. There are two variants:
  *
  *   - [[StreamPosition.Exact]] An exact position in a stream.
  *   - [[StreamPosition.End]] Represents the end of a particular stream.
  */
sealed trait StreamPosition
object StreamPosition:

  val Start: Exact = Exact(ULong.min)

  final case class Exact(value: ULong) extends StreamPosition with StreamState
  object Exact:
    def fromUnsigned(value: Long): Exact = Exact(ULong(value))

  case object End extends StreamPosition

  /** Constructs an exact stream position in a stream.
    */
  def apply(value: Long): Exact = Exact.fromUnsigned(value)

  //

  extension (sp: StreamPosition)
    def render: String = sp match
      case e: Exact => s"${e.value.render}"
      case End      => "end"

  //

  given Order[StreamPosition] = Order.from {
    case (x: Exact, y: Exact) => Order[Exact].compare(x, y)
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

  given Order[Exact] = Order.by(_.value)

//======================================================================================================================

/** Log position for the global stream. There are two variants:
  *
  *   - [[LogPosition.Exact]] An exact position in the global stream.
  *   - [[LogPosition.End]] Represents the end of the global stream.
  */
sealed trait LogPosition
object LogPosition:

  val Start: Exact = exact(0L, 0L)

  sealed abstract case class Exact(commit: ULong, prepare: ULong) extends LogPosition
  object Exact:

    val MaxValue: Exact = create(ULong.max, ULong.max)

    private[sec] def create(commit: ULong, prepare: ULong): Exact =
      new Exact(commit, prepare) {}

    def apply(commit: Long, prepare: Long): Either[InvalidInput, Exact] = {
      val commitU  = ULong(commit)
      val prepareU = ULong(prepare)
      def error    = InvalidInput(s"commit must be >= prepare, but $commitU < $prepareU")
      if (commitU < prepareU) error.asLeft else create(commitU, prepareU).asRight
    }

  case object End extends LogPosition

  //

  private[sec] def exact(commit: Long, prepare: Long): Exact =
    Exact.create(ULong(commit), ULong(prepare))

  /** Constructs an exact log position in the global stream.
    *
    * Values are validated that @param commit is larger than @param prepare.
    */
  def apply(commit: Long, prepare: Long): Either[InvalidInput, Exact] = Exact(commit, prepare)

  //

  extension (lp: LogPosition)
    def render: String = lp match
      case Exact(c, p) => s"(c = ${c.render}, p = ${p.render})"
      case End         => "end"

  //

  given Order[LogPosition] = Order.from {
    case (x: Exact, y: Exact) => Order[Exact].compare(x, y)
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

  given Order[Exact] = Order.from { (x: Exact, y: Exact) =>
    (x.commit compare y.commit, x.prepare compare y.prepare) match
      case (0, 0) => 0
      case (0, x) => x
      case (x, _) => x
  }
