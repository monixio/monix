/*
 * Copyright (c) 2014-2015 by its authors. Some rights reserved.
 * See the project homepage at: http://www.monifu.org
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

package monifu.reactive.observers

import monifu.collection.mutable.ConcurrentQueue
import monifu.concurrent.atomic.padded.Atomic
import monifu.reactive.Ack.{Cancel, Continue}
import monifu.reactive.exceptions.BufferOverflowException
import monifu.reactive.observers.SynchronousBufferedSubscriber.State
import monifu.reactive.{Ack, Subscriber}
import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
 * A highly optimized [[BufferedSubscriber]] implementation. It supports 2
 * [[monifu.reactive.OverflowStrategy overflow strategies]]:
 *
 *   - [[monifu.reactive.OverflowStrategy.Unbounded Unbounded]]
 *   - [[monifu.reactive.OverflowStrategy.Fail Fail]]
 *
 * @param underlying is the underlying observer receiving the queued events
 * @param bufferSize is the maximum buffer size, or zero if unbounded
 */
private[reactive] final class SynchronousBufferedSubscriber[-T] private
  (underlying: Subscriber[T], bufferSize: Int = 0)
  extends BufferedSubscriber[T] with SynchronousSubscriber[T] { self =>

  require(bufferSize >= 0, "bufferSize must be a positive number")

  implicit val scheduler = underlying.scheduler

  // State for managing contention between multiple producers and one consumer
  private[this] val stateRef = Atomic(State())
  // Concurrent queue, producers can push into it without extra synchronization
  // and there's a happens before relationship between `queue.offer` and
  // incrementing `stateRef.itemsToPush`, which we are using on the consumer
  // side in order to know how many items to process and when to stop
  private[this] val queue = ConcurrentQueue.empty[T]
  // Used on the consumer side to split big synchronous workloads in batches
  private[this] val batchSizeModulus = scheduler.env.batchSize - 1

  def onNext(elem: T): Ack = {
    val state = stateRef.get
    if (state.upstreamShouldStop) Cancel else
      try {
        queue.offer(elem)
        pushToConsumer(state)
      }
      catch {
        case NonFatal(ex) =>
          onError(ex)
          Cancel
      }
  }

  /**
   * Reusable logic for signaling a completion event or an error downstream,
   * used in both `onError` and `onComplete`. The `error` param should
   * be `null` for signaling a normal `onComplete`.
   */
  @tailrec private def signalCompleteOrError(ex: Throwable): Unit = {
    val state = stateRef.get

    if (!state.upstreamShouldStop) {
      // signaling the completion of upstream to the consumer's run-loop
      val update = state.copy(upstreamIsComplete = true, errorThrown = ex)
      if (stateRef.compareAndSet(state, update))
        pushToConsumer(update)
      else // CAS failed, retry
        signalCompleteOrError(ex)
    }
  }

  def onError(ex: Throwable): Unit = {
    signalCompleteOrError(ex)
  }

  def onComplete() = {
    signalCompleteOrError(null)
  }

  /**
   * Function that starts an asynchronous consumer run-loop or in case
   * a run-loop is already in progress this function increments the
   * itemsToPush count. And in case we've exceeded the bufferSize, then
   * we start to apply back-pressure.
   */
  @tailrec private def pushToConsumer(state: State): Ack = {
    // no run-loop is active? then we need to start one
    if (state.itemsToPush == 0) {
      val update = state.copy(itemsToPush = 1)
      if (!stateRef.compareAndSet(state, update)) {
        // CAS failed, retry
        pushToConsumer(stateRef.get)
      }
      else {
        scheduler.execute(new Runnable { def run() = fastLoop(update, 0, 0) })
        Continue
      }
    }
    else {
      val update = state.copy(itemsToPush = state.itemsToPush + 1)
      if (!stateRef.compareAndSet(state, update)) {
        // CAS failed, retry
        pushToConsumer(stateRef.get)
      }
      else if (bufferSize > 0 && update.itemsToPush > bufferSize && !state.upstreamIsComplete) {
        self.onError(new BufferOverflowException(
          "Downstream observer is too slow, buffer over capacity with a " +
          s"specified buffer size of $bufferSize and" +
          s" ${state.itemsToPush} events being left for push"
        ))
        
        Cancel
      }
      else {
        Continue
      }
    }
  }

  private def rescheduled(processed: Int): Unit = {
    fastLoop(stateRef.get, processed, 0)
  }

  /**
   * Starts a consumer run-loop that consumers everything we have in our
   * queue, pushing those events to our downstream observer and then stops.
   */
  @tailrec private def fastLoop(state: State, processed: Int, syncIndex: Int): Unit = {
    // should be called when signaling a complete
    def downstreamSignalComplete(ex: Throwable = null): Unit = {
      stateRef.transformAndGet(_.downstreamComplete)
      if (ex != null)
        underlying.onError(ex)
      else
        underlying.onComplete()
    }

    // We protect the downstream, only doing this as long as the downstream
    // hasn't canceled, or as long as we haven't signaled an onComplete or an
    // onError event yet
    if (!state.downstreamIsDone) {
      // Processing until we're hitting `itemsToPush`
      if (processed < state.itemsToPush) {
        val next: T = queue.poll()

        if (next != null) {
          val ack = underlying.onNext(next)
          // for establishing whether the next call is asynchronous,
          // note that the check with batchSizeModulus is meant for splitting
          // big synchronous loops in smaller batches
          val nextIndex = if (!ack.isCompleted) 0 else
            (syncIndex + 1) & batchSizeModulus

          if (nextIndex > 0) {
            if (ack == Continue || ack.value.get == Continue.IsSuccess)
              fastLoop(state, processed + 1, nextIndex)
            else if (ack == Cancel || ack.value.get == Cancel.IsSuccess) {
              // ending loop
              stateRef.transformAndGet(_.downstreamComplete)
            }
            else {
              // ending loop
              val ex = ack.value.get.failed.getOrElse(new MatchError(ack.value.get))
              downstreamSignalComplete(ex)
            }
          }
          else ack.onComplete {
            case Continue.IsSuccess =>
              // re-run loop (in different thread)
              rescheduled(processed + 1)

            case Cancel.IsSuccess =>
              // ending loop
              stateRef.transformAndGet(_.downstreamComplete)

            case failure =>
              // ending loop
              val ex = failure.failed.getOrElse(new MatchError(failure))
              downstreamSignalComplete(ex)
          }
        }
        else {
          // upstreamIsComplete=true, ending loop
          assert(state.upstreamIsComplete, "upstreamIsComplete should be true")
          try downstreamSignalComplete(state.errorThrown) finally
            queue.clear() // for GC purposes
        }
      }
      else {
        // at this point processed == itemsToPush
        val ref = state.declareProcessed(processed)
        // trying update, if it fails it probably means we've got more
        // items to process and if it succeeds it means we are done
        // note that we don't need to check that itemsToPush is zero
        if (!stateRef.compareAndSet(state, ref)) {
          // concurrent modifications happened, continuing loop
          fastLoop(stateRef.get, processed, syncIndex)
        }
      }
    }
  }
}

private[reactive] object SynchronousBufferedSubscriber {
  def unbounded[T](underlying: Subscriber[T]): SynchronousBufferedSubscriber[T] =
    new SynchronousBufferedSubscriber[T](underlying)

  def overflowTriggering[T](underlying: Subscriber[T], bufferSize: Int): SynchronousBufferedSubscriber[T] =
    new SynchronousBufferedSubscriber[T](underlying, bufferSize)

  /** State used in our implementation to manage concurrency */
  private case class State(
    itemsToPush: Int = 0,
    upstreamIsComplete: Boolean = false,
    downstreamIsDone: Boolean = false,
    errorThrown: Throwable = null) {

    def upstreamShouldStop: Boolean = {
      upstreamIsComplete || downstreamIsDone
    }

    def downstreamComplete: State = {
      copy(itemsToPush = 0, downstreamIsDone = true)
    }

    def declareProcessed(processed: Int): State = {
      copy(itemsToPush = itemsToPush - processed)
    }

    def incrementItemsToPush: State = {
      copy(itemsToPush = itemsToPush + 1)
    }
  }
}
