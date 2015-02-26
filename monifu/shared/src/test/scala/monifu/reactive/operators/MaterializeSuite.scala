/*
 * Copyright (c) 2014-2015 Alexandru Nedelcu
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

import monifu.reactive.Ack.Cancel
import monifu.reactive.Notification.{OnComplete, OnError, OnNext}
import monifu.reactive.{Notification, Observer, Observable}
import scala.concurrent.duration.Duration.Zero

object MaterializeSuite extends BaseOperatorSuite {
  def observable(sourceCount: Int) = Some {
    val o = Observable.create[Long] { s =>
      implicit val ec = s.scheduler
      val o = s.observer

      val source: Observable[Notification[Long]] =
        Observable.range(0, sourceCount).materialize

      source.subscribe(new Observer[Notification[Long]] {
        def onError(ex: Throwable) = ()
        def onComplete() = ()

        def onNext(elem: Notification[Long]) = elem match {
          case OnNext(e) =>
            o.onNext(e)
          case OnError(ex) =>
            o.onError(ex)
            Cancel
          case OnComplete =>
            o.onComplete()
            Cancel
        }
      })
    }

    Sample(o, count(sourceCount), sum(sourceCount), Zero, Zero)
  }

  def sum(sourceCount: Int): Long = sourceCount.toLong * (sourceCount - 1) / 2
  def count(sourceCount: Int) = sourceCount

  def brokenUserCodeObservable(sourceCount: Int, ex: Throwable) = None
  def observableInError(sourceCount: Int, ex: Throwable) = None
}
