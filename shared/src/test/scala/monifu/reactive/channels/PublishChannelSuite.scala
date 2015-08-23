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

package monifu.reactive.channels

import monifu.concurrent.Scheduler
import monifu.reactive.OverflowStrategy.Unbounded

object PublishChannelSuite extends BaseChannelSuite {
  def alreadyTerminatedTest(expectedElems: Seq[Long])(implicit s: Scheduler) = {
    val c = PublishChannel[Long](Unbounded)
    Sample(c, 0)
  }

  def continuousStreamingTest(expectedElems: Seq[Long])(implicit s: Scheduler) = {
    val c = PublishChannel[Long](Unbounded)
    Some(Sample(c, expectedElems.sum))
  }
}
