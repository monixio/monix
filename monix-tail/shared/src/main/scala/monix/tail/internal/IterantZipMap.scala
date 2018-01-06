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

package monix.tail.internal

import cats.effect.Sync
import cats.syntax.all._
import cats.{Applicative, Parallel}
import monix.eval.instances.ParallelApplicative
import monix.execution.misc.NonFatal
import monix.tail.Iterant
import monix.tail.Iterant.{Halt, Last, Next, NextBatch, NextCursor, Suspend}
import monix.tail.batches.{Batch, BatchCursor}
import scala.collection.mutable.ArrayBuffer


private[tail] object IterantZipMap {
  /**
    * Implementation for `Iterant#zipMap`
    */
  def seq[F[_], A, B, C](lh: Iterant[F, A], rh: Iterant[F, B], f: (A, B) => C)
    (implicit F: Sync[F]): Iterant[F, C] =
    apply(lh, rh, f)(F, F)

  /**
    * Implementation for `Iterant#parZipMap`
    */
  def par[F[_], G[_], A, B, C](lh: Iterant[F, A], rh: Iterant[F, B], f: (A, B) => C)
    (implicit F: Sync[F], P: Parallel[F, G]): Iterant[F, C] = {

    val A = ParallelApplicative(P)
    apply(lh, rh, f)(F, A)
  }

  private def apply[F[_], A, B, C](lh: Iterant[F, A], rh: Iterant[F, B], f: (A, B) => C)
    (implicit F: Sync[F], A: Applicative[F]): Iterant[F, C] = {

    def loop(lh: Iterant[F, A], rh: Iterant[F, B]): Iterant[F, C] = {
      def stopBoth(stopA: F[Unit], stopB: F[Unit]): F[Unit] =
        stopA.flatMap(_ => stopB)

      def processPair(a: A, restA: F[Iterant[F, A]], stopA: F[Unit], b: B, restB: F[Iterant[F, B]], stopB: F[Unit]) = {
        val rest = A.map2(restA, restB)(loop)
        Next(f(a, b), rest, stopBoth(stopA, stopB))
      }

      def processOneASeqB(lh: Iterant[F, A], a: A, restA: F[Iterant[F, A]], stopA: F[Unit], refB: NextCursor[F, B]): Iterant[F, C] = {
        val NextCursor(itemsB, restB, stopB) = refB
        if (!itemsB.hasNext)
          Suspend(restB.map(loop(lh, _)), stopBoth(stopA, stopB))
        else
          processPair(a, restA, stopA, itemsB.next(), F.pure(refB), stopB)
      }

      def processSeqAOneB(refA: NextCursor[F, A], rh: Iterant[F, B], b: B, restB: F[Iterant[F, B]], stopB: F[Unit]): Iterant[F, C] = {
        val NextCursor(itemsA, restA, stopA) = refA
        if (!itemsA.hasNext)
          Suspend(restA.map(loop(_, rh)), stopBoth(stopA, stopB))
        else
          processPair(itemsA.next(), F.pure(refA), stopA, b, restB, stopB)
      }

      def processSeqASeqB(refA: NextCursor[F, A], refB: NextCursor[F, B]): Iterant[F, C] = {
        val NextCursor(itemsA, restA, stopA) = refA
        val NextCursor(itemsB, restB, stopB) = refB

        // Processing multiple batch at once, but only if the iterators
        // aren't infinite, otherwise we have to process them lazily
        val batchSize = math.min(itemsA.recommendedBatchSize, itemsB.recommendedBatchSize)
        if (batchSize > 1) {
          val buffer = ArrayBuffer.empty[C]
          var toFetch = batchSize

          while (toFetch > 0 && itemsA.hasNext() && itemsB.hasNext()) {
            buffer += f(itemsA.next(), itemsB.next())
            toFetch -= 1
          }

          val isEmptyItemsA = !itemsA.hasNext()
          val isEmptyItemsB = !itemsB.hasNext()
          val array = buffer.toArray[Any]

          if (isEmptyItemsA && isEmptyItemsB) {
            if (array.isEmpty)
              Suspend(A.map2(restA, restB)(loop), stopBoth(stopA, stopB))
            else
              NextBatch(Batch.fromAnyArray(array), A.map2(restA, restB)(loop), stopBoth(stopA, stopB))
          }
          else if (isEmptyItemsA) {
            if (array.isEmpty)
              Suspend(restA.map(loop(_, refB)), stopBoth(stopA, stopB))
            else
              NextBatch(Batch.fromAnyArray(array), restA.map(loop(_, refB)), stopBoth(stopA, stopB))
          }
          else if (isEmptyItemsB) {
            if (array.isEmpty)
              Suspend(restB.map(loop(refA, _)), stopBoth(stopA, stopB))
            else
              NextBatch(Batch.fromAnyArray(array), restB.map(loop(refA, _)), stopBoth(stopA, stopB))
          }
          else {
            // We are not done, continue loop
            NextBatch(Batch.fromAnyArray(array), F.delay(loop(refA, refB)), stopBoth(stopA, stopB))
          }
        }
        else if (!itemsA.hasNext)
          Suspend(restA.map(loop(_, refB)), stopBoth(stopA, stopB))
        else if (!itemsB.hasNext)
          Suspend(restB.map(loop(refA, _)), stopBoth(stopA, stopB))
        else {
          val a = itemsA.next()
          val b = itemsB.next()
          Next(f(a, b), F.delay(loop(refA, refB)), stopBoth(stopA, stopB))
        }
      }

      def processLast(a: A, b: B, stop: F[Unit]): Iterant[F, C] = {
        val last = Last[F,C](f(a, b))
        Suspend(stop.map(_ => last), stop)
      }

      def processNextCursorA(lh: NextCursor[F, A], rh: Iterant[F, B]): Iterant[F, C] =
        rh match {
          case Next(b, restB, stopB) =>
            processSeqAOneB(lh, rh, b, restB, stopB)
          case refB @ NextCursor(_, _, _) =>
            processSeqASeqB(lh, refB)
          case NextBatch(itemsB, restB, stopB) =>
            val seqB = NextCursor(itemsB.cursor(), restB, stopB)
            processSeqASeqB(lh, seqB)
          case Suspend(restB, stopB) =>
            Suspend(restB.map(loop(lh, _)), stopBoth(lh.earlyStop, stopB))
          case Last(b) =>
            val NextCursor(itemsA, restA, stopA) = lh
            if (!itemsA.hasNext)
              Suspend(restA.map(loop(_, rh)), stopA)
            else {
              val a = itemsA.next()
              processLast(a, b, stopA)
            }
          case halt @ Halt(_) =>
            Suspend(lh.earlyStop.map(_ => halt.asInstanceOf[Iterant[F, C]]), lh.earlyStop)
        }

      def processLastASeqB(a: A, itemsB: BatchCursor[B], restB: F[Iterant[F, B]], stopB: F[Unit]): Iterant[F, C] = {
        if (!itemsB.hasNext())
          Suspend(restB.map(loop(lh, _)), stopB)
        else {
          val last = Last[F,C](f(a, itemsB.next()))
          Suspend(stopB.map(_ => last), stopB)
        }
      }

      try lh match {
        case Next(a, restA, stopA) =>
          rh match {
            case Next(b, restB, stopB) =>
              processPair(a, restA, stopA, b, restB, stopB)
            case refB @ NextCursor(_, _, stopB) =>
              processOneASeqB(lh, a, restA, stopB, refB)
            case NextBatch(itemsB, restB, stopB) =>
              val seq = NextCursor(itemsB.cursor(), restB, stopB)
              processOneASeqB(lh, a, restA, stopB, seq)
            case Suspend(restB, stopB) =>
              Suspend(restB.map(loop(lh, _)), stopBoth(stopA, stopB))
            case Last(b) =>
              processLast(a, b, stopA)
            case halt @ Halt(_) =>
              Suspend(stopA.map(_ => halt.asInstanceOf[Iterant[F,C]]), stopA)
          }

        case refA @ NextCursor(_, _, _) =>
          processNextCursorA(refA, rh)

        case NextBatch(itemsA, restA, stopA) =>
          val seq = NextCursor(itemsA.cursor(), restA, stopA)
          processNextCursorA(seq, rh)

        case Suspend(restA, stopA) =>
          rh match {
            case halt @ Halt(_) =>
              Suspend(stopA.map(_ => halt.asInstanceOf[Iterant[F, C]]), stopA)
            case Last(_) =>
              Suspend(restA.map(loop(_, rh)), stopA)
            case Suspend(restB, stopB) =>
              Suspend(A.map2(restA, restB)(loop), stopBoth(stopA, stopB))
            case _ =>
              Suspend(restA.map(loop(_, rh)), stopBoth(stopA, rh.earlyStop))
          }

        case Last(a) =>
          rh match {
            case Next(b, _, stopB) =>
              processLast(a, b, stopB)
            case NextCursor(itemsB, restB, stopB) =>
              processLastASeqB(a, itemsB, restB, stopB)
            case NextBatch(itemsB, restB, stopB) =>
              processLastASeqB(a, itemsB.cursor(), restB, stopB)
            case Suspend(restB, stopB) =>
              Suspend(restB.map(loop(lh, _)), stopB)
            case Last(b) =>
              Last(f(a, b))
            case halt @ Halt(_) =>
              halt.asInstanceOf[Iterant[F, C]]
          }

        case halt @ Halt(exA) =>
          rh match {
            case Halt(exB) =>
              Halt(exA.orElse(exB))
            case Last(_) =>
              halt.asInstanceOf[Iterant[F, C]]
            case _ =>
              Suspend(rh.earlyStop.map(_ => halt.asInstanceOf[Iterant[F, C]]), rh.earlyStop)
          }
      } catch {
        case NonFatal(ex) =>
          val stop = lh.earlyStop.flatMap(_ => rh.earlyStop)
          Suspend(stop.map(_ => Halt(Some(ex))), stop)
      }
    }

    // Given function can be side-effecting, must suspend!
    val stop = lh.earlyStop.flatMap(_ => rh.earlyStop)
    Suspend(F.delay(loop(lh, rh)), stop)
  }
}
