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
import monix.execution.{Cancelable, CancelableFuture}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * DEPRECATED — switch to Scala 2.12+ and [[monix.execution.FutureUtils]].
  */
package object execution {
  /**
    * DEPRECATED — switch to Scala 2.12+ and
    * [[monix.execution.CancelableFuture.fromJavaCompletable CancelableFuture.fromJavaCompletable]].
    */
  implicit class JavaCompletableFutureUtils[A](val source: CompletableFuture[A]) extends AnyVal {
    /**
      * DEPRECATED — switch to Scala 2.12+ and
      * [[monix.execution.CancelableFuture.fromJavaCompletable CancelableFuture.fromJavaCompletable]].
      */
    @deprecated("Switch to Scala 2.12+ and CancelableFuture.fromJavaCompletable", "3.0.1")
    def asScala(implicit ec: ExecutionContext): CancelableFuture[A] = {
      // $COVERAGE-OFF$
      CancelableFuture.async(cb => {
        source.handle[Unit](new BiFunction[A, Throwable, Unit] {
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
        Cancelable({ () => source.cancel(true); () })
      })
      // $COVERAGE-ON$
    }
  }

  /**
    * DEPRECATED — switch to Scala 2.12+ and
    * [[monix.execution.FutureUtils.toJavaCompletable FutureUtils.toJavaCompletable]].
    */
  implicit class ScalaFutureUtils[A](val source: Future[A]) extends AnyVal {
    /**
      * DEPRECATED — switch to Scala 2.12+ and
      * [[monix.execution.FutureUtils.toJavaCompletable FutureUtils.toJavaCompletable]].
      */
    @deprecated("Switch to Scala 2.12+ and FutureUtils.toJavaCompletable", "3.0.1")
    def asJava(implicit ec: ExecutionContext): CompletableFuture[A] = {
      // $COVERAGE-OFF$
      val cf = new CompletableFuture[A]()
      source.onComplete {
        case Success(a) =>
          cf.complete(a)
        case Failure(ex) =>
          cf.completeExceptionally(ex)
      }
      cf
      // $COVERAGE-ON$
    }
  }
}
