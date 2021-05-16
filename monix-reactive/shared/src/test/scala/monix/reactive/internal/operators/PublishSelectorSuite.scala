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

package monix.reactive.internal.operators

import cats.effect.IO
import monix.execution.atomic.Atomic
import monix.reactive.{BaseTestSuite, Observable, OverflowStrategy}
import scala.util.Success
import scala.concurrent.duration._

object PublishSelectorSuite extends BaseTestSuite {
  implicit val os: OverflowStrategy[Nothing] = OverflowStrategy.Default

  test("publishSelector sanity test") { implicit s =>
    val isStarted = Atomic(0)
    val f = Observable
      .range(0, 1000)
      .doOnStartF(_ => IO(isStarted.increment()))
      .publishSelector { source =>
        Observable(source, source, source).merge
      }
      .sumL[Long]
      .runToFuture

    s.tick()
    assertEquals(f.value, Some(Success(500 * 999 * 3)))
    assertEquals(isStarted.get(), 1)
  }

  test("treating Stop event") { implicit s =>
    val isStarted = Atomic(0)
    val isCanceled = Atomic(false)

    val f = Observable
      .range(0, 10000)
      .doOnStartF(_ => IO(isStarted.increment()))
      .doOnSubscriptionCancelF(() => isCanceled.set(true))
      .publishSelector { source =>
        source.map(_ => 1)
      }
      .take(2000L)
      .sumL
      .runToFuture

    s.tick()
    assertEquals(f.value, Some(Success(2000)))
    assertEquals(isStarted.get(), 1)
    assert(isCanceled.get(), "isCanceled")
  }

  test("publish selector respects subscription when used with chained operators") { implicit s =>
    val ob = Observable.now(1)
      .publishSelector(_.takeLast(1))
      .takeLast(1)

    s.tick(1.second)
    val f = ob.headL.runToFuture
    assertEquals(f.value, Some(Success(1)))
  }
}
