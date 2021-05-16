/*
 * Copyright (c) 2014-2021 by The Monix Project Developers.
 * See the project homepage at: https://monix.io
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

package monix.catnap
import monix.execution.annotations.UnsafeProtocol
import monix.execution.{BufferCapacity, ChannelType}
import monix.execution.atomic.PaddingStrategy

/**
  * A simple interface that models the consumer side of a producer-consumer
  * communication channel.
  *
  * Currently exposed by [[ConcurrentChannel.consume]].
  *
  * @tparam F is effect type used for processing tasks asynchronously
  * @tparam E is the type for the completion event
  * @tparam A is the type for the stream of events being consumed
  */
trait ConsumerF[F[_], E, A] extends Serializable {
  /**
    * Pulls one message from the communication channel, when it becomes available.
    *
    * Example:
    * {{{
    *   import cats.implicits._
    *   import cats.effect.Async
    *
    *   def sum[F[_]](channel: ConsumerF[F, Int, Int], acc: Long = 0)
    *     (implicit F: Async[F]): F[Long] = {
    *
    *     channel.pull.flatMap {
    *       case Left(e) => F.pure(acc + e)
    *       case Right(i) => sum(channel, acc + i)
    *     }
    *   }
    * }}}
    *
    * @return either `Left(e)`, if the channel was closed with a final `e`
    *         completion event, or `Right(a)`, representing a message that
    *         was pulled from the channel
    */
  def pull: F[Either[E, A]]

  /**
    * Pulls a whole batch of messages from the channel, at least one,
    * the returned sequence being no larger than the specified `maxLength`.
    *
    * {{{
    *   import cats.implicits._
    *   import cats.effect.Async
    *
    *   def sum[F[_]](channel: ConsumerF[F, Int, Int], acc: Long = 0)
    *     (implicit F: Async[F]): F[Long] = {
    *
    *     channel.pullMany(1, 16).flatMap {
    *       case Left(e) => F.pure(acc + e)
    *       case Right(seq) => sum(channel, acc + seq.sum)
    *     }
    *   }
    * }}}
    *
    * @param minLength is the minimum size of the returned sequence;
    *        for as long as the channel isn't halted, the returned task will
    *        back-pressure until the required number of events have been
    *        collected
    *
    * @param maxLength is the maximum size of the returned sequence;
    *        for fairness purposes (e.g. multiple workers consuming from
    *        the same `ConsumerF`), a smaller value is recommended,
    *        or otherwise `Int.MaxValue` can be used
    *
    * @return either `Left(e)`, if the channel was closed with a final `e`
    *         completion event, or `Right(seq)`, representing a non-empty
    *         sequence of messages pulled from the channel, but that is
    *         no larger than `maxLength`
    */
  def pullMany(minLength: Int, maxLength: Int): F[Either[E, Seq[A]]]

  /** Checks if the queue is empty.
    *
    * '''UNSAFE PROTOCOL:'''
    * Concurrent shared state changes very frequently, therefore this function might yield nondeterministic results.
    * Should be used carefully since some usecases might require a deeper insight into concurrent programming.
    */
  @UnsafeProtocol
  def isEmpty: F[Boolean]
}

object ConsumerF {
  /**
    * Custom configuration consumers, used in [[ChannelF.consumeWithConfig]].
    *
    * @param capacity configures the capacity of the underlying buffer
    *
    * @param consumerType (UNSAFE) is for fine-tuning the consumer type;
    *        if there's only one worker that consumes from the [[ConsumerF]]
    *        value, then a [[monix.execution.ChannelType.SingleConsumer SingleConsumer]]
    *        configuration is possible, for optimization purposes, otherwise
    *        or if in doubt, prefer
    *        [[monix.execution.ChannelType.MultiConsumer MultiConsumer]]
    *
    * @param padding is the padding strategy used for the atomics meant for
    *        back-pressuring on the internal buffer (either waiting on
    *        new events on the consumer side, or waiting until the buffer
    *        has room for pushing events on the producer side); this is for
    *        fine-tuning the configuration, but note that padding adds some
    *        bytes to the the memory being consumed, so a "no padding" strategy
    *        may be what you want
    */
  final case class Config(
    capacity: Option[BufferCapacity] = None,
    consumerType: Option[ChannelType.ConsumerSide] = None,
    padding: Option[PaddingStrategy] = None
  )

  object Config {
    val default = Config()
  }
}
