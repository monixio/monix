/*
 * Copyright (c) 2014-2019 by The Monix Project Developers.
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

package monix.tail

import cats.laws._
import cats.laws.discipline._
import monix.eval.Coeval

object IterantRetryIfEmptySuite extends BaseTestSuite {
  test("Iterant.pure(1).retryIfEmpty mirrors source") { _ =>
    val r = Iterant[Coeval].pure(1).retryIfEmpty(None).toListL.value()
    assertEquals(r, List(1))
  }

  test("Iterant.suspend(Iterant.pure(1)).retryIfEmpty mirrors source") { _ =>
    val r = Iterant.suspend(Iterant[Coeval].pure(1)).retryIfEmpty(None).toListL.value()
    assertEquals(r, List(1))
  }

  test("(batch ++ batch ++ batch ++ batch).retryIfEmpty mirrors source") { _ =>
    def batch(start: Int) = Iterant[Coeval].fromArray(Array(start, start + 1, start + 2))
    val r = (batch(1) ++ batch(4) ++ batch(7) ++ batch(10)).retryIfEmpty(None).toListL.value()
    assertEquals(r, (1 to 12).toList)
  }

  test("iterant.retryIfEmpty <-> iterant (for pure streams)") { _ =>
    check1 { (stream: Iterant[Coeval, Int]) =>
      stream.retryIfEmpty(Some(1)) <-> stream
    }
  }

  test("iterant.retryIfEmpty handles Scopes properly") { _ =>
    var cycles = 0
    var acquired = 0
    val empty = Iterant[Coeval].suspend(Coeval {
      cycles += 1
      Iterant[Coeval].empty[Int]
    })

    val resource = Iterant[Coeval].resource(Coeval(acquired += 1))(_ => Coeval {
      assertEquals(acquired, 1)
      acquired -= 1
    }).flatMap(_ => Iterant[Coeval].empty[Int])

    val r = (empty ++ resource).retryIfEmpty(Some(2)).toListL.value()
    assertEquals(r, Nil)
    assertEquals(cycles, 3)
    assertEquals(acquired, 0)
  }
}
