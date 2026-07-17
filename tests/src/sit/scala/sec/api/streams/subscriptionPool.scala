/*
 * Copyright 2020 Scala Event Sourcing Client for KurrentDB
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

import scala.concurrent.duration.*
import cats.syntax.all.*
import cats.effect.{IO, Resource}
import fs2.Stream
import sec.syntax.all.*
import sec.api.exceptions.SubscriptionPoolExhausted
import sec.api.pool.{Limit, PoolConfig}

/** End-to-end coverage of the pooled subscription transport: subscriptions route over dedicated channels with at most
  * `streamsPerChannel` concurrent streams each, growing the pool as needed, while reads and appends stay on the regular
  * channel.
  */
class SubscriptionPoolSuite extends SnSuite:

  import StreamState.*

  def pooledClient(streamsPerChannel: Int, maxChannels: Int): Resource[IO, EsClient[IO]] =
    Resource
      .eval(PoolConfig.of[IO](streamsPerChannel, Limit.Bounded(maxChannels)))
      .flatMap(pc => SnSuite.mkClient[IO](log, _.withSubscriptionPool(pc)))

  // streamsPerChannel = 2 forces the pool to grow to ceil(subscribers / 2) channels.
  override def makeResource: Resource[IO, EsClient[IO]] =
    pooledClient(streamsPerChannel = 2, maxChannels = 4)

  val streamPrefix = s"streams_subscription_pool_${genIdentifier}_"

  test("concurrent subscriptions flow over pooled channels; reads and appends unaffected") {

    val subscriberCount = 5 // 3 pooled channels at 2 streams each
    val events          = genEvents(10)

    def one(i: Int): IO[Unit] =
      val id        = genStreamId(s"${streamPrefix}flow_${i}_")
      val subscribe = streams.subscribeToStream(id, None).take(events.size.toLong).map(_.eventData)
      val write     = Stream.eval(streams.appendToStream(id, NoStream, events)).delayBy(500.millis)

      subscribe
        .concurrently(write)
        .compile
        .toList
        .flatMap { received =>
          IO(assertEquals(received, events.toList)) *>
            streams
              .readStream(id, StreamPosition.Start, Direction.Forwards, events.size.toLong, false)
              .compile
              .count
              .flatMap(n => IO(assertEquals(n, events.size.toLong)))
        }

    (0 until subscriberCount).toList.parTraverse_(one)
  }

  test("bounded pool rejects loudly when exhausted") {
    pooledClient(streamsPerChannel = 1, maxChannels = 1).use { client =>
      val holder = client.streams.subscribeToStream(genStreamId(s"${streamPrefix}exhausted_1_"), None)
      val second = client.streams.subscribeToStream(genStreamId(s"${streamPrefix}exhausted_2_"), None)

      holder.compile.drain.background.use { _ =>
        IO.sleep(2.seconds) *> // let the holder claim the single permit
          second.compile.drain.attempt.flatMap {
            case Left(SubscriptionPoolExhausted(1)) => IO.unit
            case other                              => IO(fail(s"expected SubscriptionPoolExhausted(1), got $other"))
          }
      }
    }
  }
