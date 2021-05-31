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

import cats.syntax.all._
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
  * 122Use Cases122
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

  implicit val eq: Eq[StreamState] = Eq.fromUniversalEquals

  def render(ss: StreamState): String = ss match {
    case NoStream                => "NoStream"
    case Any                     => "Any"
    case StreamExists            => "StreamExists"
    case StreamPosition.Exact(v) => s"Exact(${v}L)"
  }

  implicit final class StreamStateOps(val ss: StreamState) extends AnyVal {
    def render: String = StreamState.render(ss)
  }

}

//======================================================================================================================

/** Stream position in an individual stream. There are two variants:
  *
  *   - [[StreamPosition.Exact]] An exact position in a stream.
  *   - [[StreamPosition.End]] Represents the end of a particular stream.
  */
sealed trait StreamPosition
object StreamPosition {

  val Start: Exact = exact(0L)

  sealed abstract case class Exact(value: Long) extends StreamPosition with StreamState with PositionInfo
  object Exact {

    private[StreamPosition] def create(value: Long): Exact = new Exact(value) {}

    def apply(value: Long): Either[InvalidInput, Exact] =
      Either.cond(value >= 0L, create(value), InvalidInput(s"value must be >= 0, but is $value"))
  }

  case object End extends StreamPosition

  ///

  private[sec] def exact(value: Long): Exact = Exact.create(value)

  /** Constructs an exact stream position in a stream. Provided value is validated for `0L` <= `Long.MaxValue`.
    */
  def apply(value: Long): Either[InvalidInput, Exact] = Exact(value)

  ///

  implicit final class StreamPositionOps(val sp: StreamPosition) extends AnyVal {
    def render: String = sp match {
      case e: Exact => s"${e.value}L"
      case End      => "end"
    }
  }

  ///

  implicit val orderForStreamPosition: Order[StreamPosition] = Order.from {
    case (x: Exact, y: Exact) => Order[Exact].compare(x, y)
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

  implicit val orderForExact: Order[Exact] = Order.by(_.value)

}

//======================================================================================================================

/** Log position for the global stream. There are two variants:
  *
  *   - [[LogPosition.Exact]] An exact position in the global stream.
  *   - [[LogPosition.End]] Represents the end of the global stream.
  */
sealed trait LogPosition
object LogPosition {

  val Start: Exact = exact(0L, 0L)

  sealed abstract case class Exact(commit: Long, prepare: Long) extends LogPosition
  object Exact {

    private[LogPosition] def create(commit: Long, prepare: Long): Exact =
      new Exact(commit, prepare) {}

    def apply(commit: Long, prepare: Long): Either[InvalidInput, Exact] = {

      val result = for {
        c <- Either.cond(commit >= 0, commit, s"commit must be >= 0, but is $commit")
        p <- Either.cond(prepare >= 0, prepare, s"prepare must be >= 0, but is $prepare")
        e <- Either.cond(commit >= prepare, create(c, p), s"commit must be >= prepare, but $commit < $prepare")
      } yield e

      result.leftMap(InvalidInput(_))
    }

  }

  case object End extends LogPosition

  ///

  private[sec] def exact(commit: Long, prepare: Long): Exact = Exact.create(commit, prepare)

  /** Constructs an exact log position in the global stream. Provided values are validated for `0L` <= `Long.MaxValue`
    * and that @param commit is larger than @param prepare.
    */
  def apply(commit: Long, prepare: Long): Either[InvalidInput, Exact] = Exact(commit, prepare)

  ///

  implicit final class LogPositionOps(val lp: LogPosition) extends AnyVal {
    def render: String = lp match {
      case Exact(c, p) => s"(c = ${c}L, p = ${p}L)"
      case End         => "end"
    }
  }

  ///

  implicit val orderForLogPosition: Order[LogPosition] = Order.from {
    case (x: Exact, y: Exact) => Order[Exact].compare(x, y)
    case (_: Exact, End)      => -1
    case (End, _: Exact)      => 1
    case (End, End)           => 0
  }

  implicit val orderForExact: Order[Exact] = Order.from { (x: Exact, y: Exact) =>
    (x.commit compare y.commit, x.prepare compare y.prepare) match {
      case (0, 0) => 0
      case (0, x) => x
      case (x, _) => x
    }
  }

}

//======================================================================================================================

sealed trait PositionInfo
object PositionInfo {

  final case class Global(
    stream: StreamPosition.Exact,
    log: LogPosition.Exact
  ) extends PositionInfo

  type Local = StreamPosition.Exact

  implicit final class PositionOps(val p: PositionInfo) extends AnyVal {

    def fold[A](global: Global => A, local: Local => A): A = p match {
      case a: Global => global(a)
      case l: Local  => local(l)
    }

    def streamPosition: StreamPosition.Exact = fold(_.stream, identity)

    def renderPosition: String = fold(
      a => s"log: (c = ${a.log.commit}L, p = ${a.log.prepare}L), stream: ${a.stream.value}L",
      e => s"stream: ${e.value}L"
    )

  }

}
