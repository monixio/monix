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

package monix.eval
package internal

import cats.effect.IO
import monix.execution.{Cancelable, Scheduler, UncaughtExceptionReporter}
import monix.execution.cancelables.{SingleAssignCancelable, StackedCancelable}
import monix.execution.internal.AttemptCallback
import monix.execution.internal.AttemptCallback.noop

/** INTERNAL API
  *
  * `Task` integration utilities for the `cats.effect.ConcurrentEffect`
  * instance, provided in `monix.eval.instances`.
  */
private[eval] object TaskEffect {
  /**
    * `cats.effect.Async#async`
    */
  def async[A](k: (Either[Throwable, A] => Unit) => Unit): Task[A] =
    Task.unsafeCreate { (ctx, cb) =>
      implicit val sc = ctx.scheduler
      k {
        case Right(a) => cb.asyncOnSuccess(a)
        case Left(e) => cb.asyncOnError(e)
      }
    }

  /**
    * `cats.effect.Concurrent#cancelable`
    */
  def cancelable[A](k: (Either[Throwable, A] => Unit) => IO[Unit]): Task[A] =
    Task.unsafeCreate { (ctx, cb) =>
      implicit val sc = ctx.scheduler
      val conn = ctx.connection
      val cancelable = SingleAssignCancelable()
      conn push cancelable

      val io = k(new CreateCallback[A](conn, cb))
      if (io != IO.unit) cancelable := new CancelableIO(io)
    }

  /**
    * `cats.effect.Effect#runAsync`
    */
  def runAsync[A](fa: Task[A])(cb: Either[Throwable, A] => IO[Unit])
    (implicit sc: Scheduler): IO[Unit] =
    IO { execute(fa, cb); () }

  /**
    * `cats.effect.ConcurrentEffect#runCancelable`
    */
  def runCancelable[A](fa: Task[A])(cb: Either[Throwable, A] => IO[Unit])
    (implicit sc: Scheduler): IO[IO[Unit]] =
    IO(execute(fa, cb).cancelIO)

  private def execute[A](fa: Task[A], cb: Either[Throwable, A] => IO[Unit])
    (implicit sc: Scheduler) = {

    fa.runAsync(new Callback[A] {
      def onSuccess(value: A): Unit =
        cb(Right(value)).unsafeRunAsync(noop)
      def onError(ex: Throwable): Unit =
        cb(Left(ex)).unsafeRunAsync(noop)
    })
  }

  private final class CreateCallback[A](
    conn: StackedCancelable, cb: Callback[A])
    (implicit sc: Scheduler)
    extends (Either[Throwable, A] => Unit) {

    override def apply(value: Either[Throwable, A]): Unit = {
      conn.pop()
      cb.asyncApply(value)
    }
  }

  /** Cancelable instance for converting `IO` references
    *
    * This does not guarantee idempotency, because we don't need to
    * (the `SingleAssignCancelable` is enough).
    */
  private final class CancelableIO(io: IO[Unit])
    (implicit val r: UncaughtExceptionReporter)
    extends Cancelable {

    def cancel(): Unit =
      io.unsafeRunAsync(AttemptCallback.empty)
  }
}