/*
 * Copyright (c) 2015 Alexandru Nedelcu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package monifu.reactive.operators

import monifu.concurrent.{Scheduler, Cancelable}
import monifu.reactive.Ack.{Cancel, Continue}
import monifu.reactive.{Ack, Observer, Observable}
import monifu.reactive.observers.SynchronousObserver
import monifu.reactive.internals._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

object sample {
  def once2[T](source: Observable[T], initialDelay: FiniteDuration, delay: FiniteDuration): Observable[T] =
    Observable.create { subscriber =>
      implicit val s = subscriber.scheduler
      val observer = subscriber.observer

      source.unsafeSubscribe(new Observer[T] {
        private[this] var lastValue: T = _
        @volatile private[this] var hasValue = false
        @volatile private[this] var upstreamIsDone = false
        private[this] val task = tick(initialDelay, delay)

        def tick(initialDelay: FiniteDuration, period: FiniteDuration): Cancelable =
          s.scheduleOnce(initialDelay, {
            val startedAt = s.nanoTime

            if (hasValue) {
              val result = observer.onNext(lastValue)
              hasValue = false

              result.onContinue {
                val duration = (s.nanoTime - startedAt).nanos
                val delay = {
                  val d = period - duration
                  if (d >= Duration.Zero) d else Duration.Zero
                }
                
                tick(delay, period)
              }
            }
            else {
              tick(period, period)
            }
          })


        def onNext(elem: T): Future[Ack] = {

        }

        def onError(ex: Throwable): Unit = ???

        def onComplete(): Unit = ???
      })
    }


  /**
   * Implementation for `Observable.sample(initialDelay, delay)`.
   *
   * By comparison with [[monifu.reactive.Observable.sampleRepeated]],
   * this version does not emit any events if no fresh values were emitted
   * since the last sampling.
   */
  def once[T](source: Observable[T], initialDelay: FiniteDuration, delay: FiniteDuration): Observable[T] =
    Observable.create { subscriber =>
      implicit val s = subscriber.scheduler
      val observer = subscriber.observer

      source.unsafeSubscribe(new SampleObserver(
        observer,
        initialDelay,
        delay,
        shouldRepeatOnSilence = false
      ))
    }

  /**
   * Implementation for `Observable.sampleRepeated(initialDelay, delay)`.
   *
   * By comparison with [[monifu.reactive.Observable.sample]], this version always
   * emits values at the requested interval, even if no fresh value in the meantime.
   */
  def repeated[T](source: Observable[T], initialDelay: FiniteDuration, delay: FiniteDuration): Observable[T] =
     Observable.create { subscriber =>
       implicit val s = subscriber.scheduler
       val observer = subscriber.observer

       source.unsafeSubscribe(new SampleObserver(
        observer,
        initialDelay,
        delay,
        shouldRepeatOnSilence = true
      ))
    }

  protected[reactive] class SampleObserver[T]
      (downstream: Observer[T], initialDelay: FiniteDuration, delay: FiniteDuration, shouldRepeatOnSilence: Boolean)
      (implicit s: Scheduler)
    extends SynchronousObserver[T] {

    @volatile private[this] var hasValue = false
    // MUST BE written before `hasValue = true`
    private[this] var lastValue: T = _

    // to be written in onComplete/onError, to be read from tick
    @volatile private[this] var upstreamIsDone = false
    // MUST BE written to before `upstreamIsDone = true`
    private[this] var upstreamError: Throwable = null

    /**
     * Function that gets scheduled for periodic execution.
     */
    def tick(initialDelay: FiniteDuration, period: FiniteDuration): Unit = {
      if (upstreamIsDone) {
        if (upstreamError != null)
          downstream.onError(upstreamError)
        else
          downstream.onComplete()
      }
      else {
        s.scheduleOnce(initialDelay, {
          val startedAt = s.nanoTime

          if (hasValue) {
            val result = downstream.onNext(lastValue)
            hasValue = shouldRepeatOnSilence

            result.onContinue {
              val duration = (s.nanoTime - startedAt).nanos
              val delay = {
                val d = period - duration
                if (d >= Duration.Zero) d else Duration.Zero
              }

              tick(delay, period)
            }
          }
          else {
            tick(period, period)
          }
        })
      }
    }

    def onNext(elem: T): Ack = {
      lastValue = elem
      hasValue = true
      Continue
    }

    def onError(ex: Throwable): Unit = {
      upstreamError = ex
      upstreamIsDone = true
    }

    def onComplete(): Unit = {
      upstreamIsDone = true
    }
  }
}
