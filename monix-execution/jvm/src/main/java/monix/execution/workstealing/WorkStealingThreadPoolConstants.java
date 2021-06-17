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

/*
 * This code is a direct copy of the Cats Effect 3 source code, donated by the
 * code authors and maintainers of the original code.
 *
 * The original Cats Effect 3 source code can be found at
 * `https://github.com/typelevel/cats-effect/blob/27bb3fdffe86d299b8966eb352fa1b2fd8125395/core/jvm/src/main/java/cats/effect/unsafe/WorkStealingThreadPoolConstants.java`.
 */

package monix.execution.workstealing;

final class WorkStealingThreadPoolConstants {

  /**
   * The number of unparked threads is encoded as an unsigned 16 bit number in the
   * 16 most significant bits of a 32 bit integer.
   */
  static final int UnparkShift = 16;

  /**
   * Constant used when parking a thread which was not searching for work.
   */
  static final int DeltaNotSearching = 1 << UnparkShift;

  /**
   * Constant used when parking a thread which was previously searching for work
   * and also when unparking any worker thread.
   */
  static final int DeltaSearching = DeltaNotSearching | 1;

  /**
   * The number of threads currently searching for work is encoded as an unsigned
   * 16 bit number in the 16 least significant bits of a 32 bit integer. Used for
   * extracting the number of searching threads.
   */
  static final int SearchMask = (1 << UnparkShift) - 1;

  /**
   * Used for extracting the number of unparked threads.
   */
  static final int UnparkMask = ~SearchMask;

  /**
   * Used for checking for new fibers from the overflow queue every few
   * iterations.
   */
  static final int OverflowQueueTicks = 64;
  static final int OverflowQueueTicksMask = OverflowQueueTicks - 1;
}
