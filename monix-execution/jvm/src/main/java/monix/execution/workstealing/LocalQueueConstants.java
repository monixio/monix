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
 * `https://github.com/typelevel/cats-effect/blob/27bb3fdffe86d299b8966eb352fa1b2fd8125395/core/jvm/src/main/java/cats/effect/unsafe/LocalQueueConstants.java`.
 */

package monix.execution.workstealing;

/**
 * Contains static constants used throughout the implementation of the
 * [[monix.execution.workstealing.LocalQueue]].
 */
class LocalQueueConstants {

  /**
   * Fixed capacity of the [[monix.execution.workstealing.unsafe.LocalQueue]] implementation,
   * empirically determined to provide a balance between memory footprint and
   * enough headroom in the face of bursty workloads which spawn a lot of fibers
   * in a short period of time.
   * 
   * Must be a power of 2.
   */
  static final int LocalQueueCapacity = 256;

  /**
   * Bitmask used for indexing into the circular buffer.
   */
  static final int LocalQueueCapacityMask = LocalQueueCapacity - 1;

  /**
   * Half of the local queue capacity.
   */
  static final int HalfLocalQueueCapacity = LocalQueueCapacity / 2;

  /**
   * Overflow fiber batch size.
   */
  static final int OverflowBatchSize = HalfLocalQueueCapacity + 1;

  /**
   * Bitmask used to extract the 16 least significant bits of a 32 bit integer
   * value.
   */
  static final int UnsignedShortMask = (1 << 16) - 1;
}
