/*
 * Copyright (c) 2014-2020 by The Monix Project Developers.
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

package monix.catnap.internal

import cats.effect.{Async}

private[monix] object AsyncUtils {
  /**
    * The `cancelable` builder from cats-effect 2
    */
  def cancelable[F[_], A](k: (Either[Throwable, A] => Unit) => F[Unit])(implicit F: Async[F]): F[A] =
    F.async[A] { cb => F.pure(Some(k(cb))) }

  /**
    * The `asyncF` builder from cats-effect 2
    */
  def asyncF[F[_], A](register: (Either[Throwable, A] => Unit) => F[Unit])(implicit F: Async[F]): F[A] =
    F.async[A] { cb => F.map(register(cb))(asNone) }

  private[this] val asNone = (_: Any) => None
}
