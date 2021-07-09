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

package monix.java8

import java.util.concurrent.{CancellationException, CompletableFuture, CompletionException}
import java.util.function.BiFunction
import monix.eval.Task
import monix.execution.{Cancelable, Scheduler}
import scala.util.{Failure, Success}

/**
  * DEPRECATED — switch to Scala 2.12+ and [[monix.eval.Task.from Task.from]].
  */
package object eval {
  /**
    * DEPRECATED — switch to Scala 2.12+ and [[monix.eval.Task.from Task.from]].
    */
  implicit class TaskCompanionUtils(val source: Task.type) extends AnyVal {
    /**
      * DEPRECATED — switch to Scala 2.12+ and [[monix.eval.Task.from Task.from]].
      */
    @deprecated("Switch to Scala 2.12+ and Task.from", "3.0.1")
    def fromCompletableFuture[A](cf: CompletableFuture[A]): Task[A] = {
      // $COVERAGE-OFF$
      convert(cf)
      // $COVERAGE-OFF$
    }

    /**
      * DEPRECATED — switch to Scala 2.12+ and [[monix.eval.Task.from Task.from]].
      */
    @deprecated("Switch to Scala 2.12+ and Task.from", "3.0.1")
    def deferCompletableFutureAction[A](f: Scheduler => CompletableFuture[A]): Task[A] = {
      // $COVERAGE-OFF$
      Task.deferAction { sc =>
        convert(f(sc))
      }
      // $COVERAGE-ON$
    }
  }

  private def convert[A](cf: CompletableFuture[A]): Task[A] = {
    // $COVERAGE-OFF$
    Task.create((_, cb) => {
      cf.handle[Unit](new BiFunction[A, Throwable, Unit] {
        override def apply(result: A, err: Throwable): Unit = {
          err match {
            case null =>
              cb(Success(result))
            case _: CancellationException =>
              ()
            case ex: CompletionException if ex.getCause ne null =>
              cb(Failure(ex.getCause))
            case ex =>
              cb(Failure(ex))
          }
        }
      })
      Cancelable({ () => cf.cancel(true); () })
    })
    // $COVERAGE-ON$
  }
}
