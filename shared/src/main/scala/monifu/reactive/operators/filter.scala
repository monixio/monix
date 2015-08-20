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

package monifu.reactive.operators

import monifu.reactive.Ack.{Cancel, Continue}
import monifu.reactive.{Observer, Observable}
import scala.concurrent.Future
import scala.util.control.NonFatal

object filter {
  /**
   * Implementation for [[Observable.filter]].
   */
  def apply[T](source: Observable[T])(p: T => Boolean): Observable[T] = {
    Observable.create { subscriber =>
      implicit val s = subscriber.scheduler
      val observer = subscriber.observer

      source.unsafeSubscribe(new Observer[T] {
        def onNext(elem: T) = {
          // Protects calls to user code from within the operator
          var streamError = true
          try {
            if (p(elem)) {
              streamError = false
              observer.onNext(elem)
            }
            else
              Continue
          }
          catch {
            case NonFatal(ex) =>
              if (streamError) { observer.onError(ex); Cancel } else Future.failed(ex)
          }
        }

        def onError(ex: Throwable) =
          observer.onError(ex)

        def onComplete() =
          observer.onComplete()
      })
    }
  }
}
