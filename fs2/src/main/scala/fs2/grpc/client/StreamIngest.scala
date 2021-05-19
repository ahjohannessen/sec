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

package fs2
package grpc
package client

import cats.implicits._
import cats.effect.kernel.{Concurrent, Ref}
import cats.effect.std.Queue

private[client] trait StreamIngest[F[_], T] {
  def onMessage(msg: T): F[Unit]
  def onClose(status: GrpcStatus): F[Unit]
  def messages: Stream[F, T]
}

private[client] object StreamIngest {

  def apply[F[_]: Concurrent, T](
    request: Int => F[Unit],
    prefetchN: Int
  ): F[StreamIngest[F, T]] =
    (Concurrent[F].ref(prefetchN), Queue.unbounded[F, Either[GrpcStatus, T]])
      .mapN((d, q) => create[F, T](request, prefetchN, d, q))

  def create[F[_], T](
    request: Int => F[Unit],
    prefetchN: Int,
    demand: Ref[F, Int],
    queue: Queue[F, Either[GrpcStatus, T]]
  )(implicit F: Concurrent[F]): StreamIngest[F, T] = new StreamIngest[F, T] {

    def onMessage(msg: T): F[Unit] =
      decreaseDemandBy(1) *> queue.offer(msg.asRight) *> ensureMessages(prefetchN)

    def onClose(status: GrpcStatus): F[Unit] =
      queue.offer(status.asLeft)

    def ensureMessages(nextWhenEmpty: Int): F[Unit] =
      (demand.get, queue.size).mapN((cd, qs) => fetch(nextWhenEmpty).whenA((cd + qs) < 1)).flatten

    def decreaseDemandBy(n: Int): F[Unit] =
      demand.update(d => math.max(d - n, 0))

    def increaseDemandBy(n: Int): F[Unit] =
      demand.update(_ + n)

    def fetch(n: Int): F[Unit] =
      request(n) *> increaseDemandBy(n)

    val messages: Stream[F, T] = {

      val run: F[Option[T]] =
        queue.take.flatMap {
          case Right(v) => v.some.pure[F] <* ensureMessages(prefetchN)
          case Left(GrpcStatus(status, trailers)) =>
            if (!status.isOk) F.raiseError(status.asRuntimeException(trailers))
            else none[T].pure[F]
        }

      Stream.repeatEval(run).unNoneTerminate

    }

  }

}
