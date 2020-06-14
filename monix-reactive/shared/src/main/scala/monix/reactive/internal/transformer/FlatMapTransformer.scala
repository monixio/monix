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

package monix.reactive.internal.transformer

import monix.reactive.{Observable, Transformer}


class FlatMapTransformer[A, B, I](f: A => Observable[B], previous: ChainableT[_, A, I]) extends Transformer[A, B, I](previous) {

  override def apply(v1: Observable[A]): Observable[B] = {
    v1.flatMap(f)
  }

}

