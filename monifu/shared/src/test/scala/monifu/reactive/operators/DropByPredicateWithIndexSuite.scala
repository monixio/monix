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

import monifu.reactive.Observable
import scala.concurrent.duration.Duration

object DropByPredicateWithIndexSuite extends BaseOperatorSuite {
  val waitForFirst = Duration.Zero
  val waitForNext = Duration.Zero

  def observable(sourceCount: Int) = {
    require(sourceCount > 0, "sourceCount should be strictly positive")
    Some {
      Observable.range(1, sourceCount * 2)
        .dropWhileWithIndex((e, idx) => e < sourceCount)
    }
  }

  def sum(sourceCount: Int): Long =
    (1 until sourceCount * 2).drop(sourceCount-1).sum

  def count(sourceCount: Int) =
    sourceCount

  def observableInError(sourceCount: Int, ex: Throwable) = {
    require(sourceCount > 0, "sourceCount should be strictly positive")
    Some {
      createObservableEndingInError(Observable.range(1, sourceCount + 2), ex)
        .dropWhileWithIndex((e, idx) => idx == 0)
    }
  }

  def brokenUserCodeObservable(sourceCount: Int, ex: Throwable) =
    None
}