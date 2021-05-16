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

package monix.reactive.observables

import monix.execution.Cancelable
import monix.execution.cancelables.{AssignableCancelable, MultiAssignCancelable}
import monix.reactive.Observable
import monix.reactive.observers.Subscriber

/** A `StackedObservable` is an [[Observable]] type used in operators that
  * end up being used in loops and that need to be memory safe
  * (e.g. `++`, `suspend`).
  *
  * This is achieved with the same trick used in the [[monix.eval.Task]]
  * implementation. The problem with recursive operators is that they
  * are leaving work behind due to the need to return a `Cancelable`
  * and thus leak memory.
  *
  * As a general rule of thumb, not all observables can be chain-able.
  * For example if the received `Subscriber` gets wrapped in another
  * subscriber instance, then chaining will not be safe unless
  * the `onNext`, `onError` and `onComplete` events introduce
  * asynchronous boundaries as well, which is overkill.
  */
abstract class ChainedObservable[+A] extends Observable[A] {
  /** Alternative subscription method, that gets injected a stacked
    * cancelable, in order to preserve memory safety.
    *
    * @param conn is a multi-assign cancelable that is passed around
    *        between `ChainedObservable` instances, being updated with
    *        the references of the current cancelable
    *
    * @param subscriber is the subscriber that will receive all events
    *        from the source
    */
  def unsafeSubscribeFn(conn: AssignableCancelable.Multi, subscriber: Subscriber[A]): Unit

  override def unsafeSubscribeFn(subscriber: Subscriber[A]): Cancelable = {
    val conn = MultiAssignCancelable()
    unsafeSubscribeFn(conn, subscriber)
    conn
  }
}

object ChainedObservable {
  /** Function that checks if the `source` is a `ChainedObservable`,
    * subscribing to it by injecting the provided `conn` if it is,
    * otherwise it triggers a normal subscription.
    */
  def subscribe[A](source: Observable[A], conn: AssignableCancelable.Multi, out: Subscriber[A]): Unit = {
    // Subscription can be synchronous, which can trigger really
    // weird ordering issues, therefore we need a light async boundary
    // to delay it and force ordering; plus it protects from stack overflows
      source match {
        case _: ChainedObservable[_] =>
          out.scheduler.executeTrampolined { () =>
            source.asInstanceOf[ChainedObservable[A]].unsafeSubscribeFn(conn, out)
          }
        case _ =>
          conn := source.unsafeSubscribeFn(out)
          ()
    }
  }
}
