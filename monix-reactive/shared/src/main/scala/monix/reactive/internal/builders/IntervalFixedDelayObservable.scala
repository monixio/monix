/*
 * Copyright (c) 2014-2018 by The Monix Project Developers.
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

package monix.reactive.internal.builders

import monix.execution.cancelables.MultiAssignCancelable
import monix.execution.{Cancelable, Ack}
import monix.execution.Ack.{Stop, Continue}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

private[reactive] final class IntervalFixedDelayObservable
  (initialDelay: FiniteDuration, delay: FiniteDuration)
  extends Observable[Long] {

  def unsafeSubscribeFn(subscriber: Subscriber[Long]): Cancelable = {
    import subscriber.{scheduler => s}

    val o = subscriber
    val task = MultiAssignCancelable()

    val runnable = new Runnable { self =>
      private[this] var counter = 0L

      def scheduleNext(): Cancelable = {
        counter += 1
        // No need to synchronize, since we have a happens-before
        // relationship between scheduleOnce invocations.
        task := s.scheduleOnce(delay.length, delay.unit, self)
      }

      def asyncScheduleNext(r: Future[Ack]): Unit =
        r.onComplete {
          case Success(ack) =>
            if (ack == Continue) scheduleNext()
          case Failure(ex) =>
            s.reportFailure(ex)
        }

      def run(): Unit = {
        val ack = o.onNext(counter)

        if (ack == Continue)
          scheduleNext()
        else if (ack != Stop)
          asyncScheduleNext(ack)
      }
    }

    if (initialDelay.length <= 0)
      runnable.run()
    else
      task := s.scheduleOnce(initialDelay.length, initialDelay.unit, runnable)

    task
  }
}
