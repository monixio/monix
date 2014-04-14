package monifu.rx.sync

import monifu.concurrent.cancelables.{CompositeCancelable, RefCountCancelable}
import monifu.rx.sync.observers.{SynchronizedObserver, AnonymousObserver}
import scala.annotation.tailrec
import monifu.concurrent.locks.NaiveSpinLock
import scala.collection.mutable
import scala.concurrent.{Promise, Future, ExecutionContext}
import monifu.concurrent.atomic.Atomic
import monifu.concurrent.cancelables.SingleAssignmentCancelable
import monifu.concurrent.Cancelable
import scala.util.control.NonFatal
import monifu.rx.common.Ack
import Ack.{Continue, Stop}
import scala.util.{Failure, Success, Try}
import monifu.rx.common.Ack


/**
 * The Observable interface that implements the Rx pattern.
 *
 * Observables are characterized by their `subscribe` function,
 * that must be overwritten for custom operators or for custom
 * Observable implementations and on top of which everything else
 * is built.
 */
trait Observable[+A]  {
  /**
   * Function that creates the actual subscription when calling `subscribe`,
   * and that starts the stream, being meant to be overridden in custom combinators
   * or in classes implementing Observable.
   *
   * @param observer is an [[monifu.rx.sync.Observer Observer]] on which `onNext`, `onComplete` and `onError`
   *                 happens, according to the Rx grammar.
   *
   * @return a cancelable that can be used to cancel the streaming
   */
  def subscribe(observer: Observer[A]): Cancelable

  final def subscribe(nextFn: A => Unit): Cancelable =
    subscribe(AnonymousObserver(nextFn))

  final def subscribe(nextFn: A => Unit, errorFn: Throwable => Unit): Cancelable =
    subscribe(AnonymousObserver(nextFn, errorFn))

  final def subscribe(nextFn: A => Unit, errorFn: Throwable => Unit, completedFn: () => Unit): Cancelable =
    subscribe(AnonymousObserver(nextFn, errorFn, completedFn))

  /**
   * Returns an Observable that applies the given function to each item emitted by an
   * Observable and emits the result.
   *
   * @param f a function to apply to each item emitted by the Observable
   * @return an Observable that emits the items from the source Observable, transformed by the given function
   */
  def map[B](f: A => B): Observable[B] =
    Observable.create(observer =>
      subscribe(new Observer[A] {
        def onNext(elem: A) = {
          // See Section 6.4. - Protect calls to user code from within an operator - in the Rx Design Guidelines
          // Note: onNext must not be protected, as it's on the edge of the monad and protecting it yields weird effects
          var streamError = true
          try {
            val r = f(elem)
            streamError = false
            observer.onNext(r)
          }
          catch {
            case NonFatal(ex) if streamError =>
              observer.onError(ex)
              Stop
          }
        }
        def onError(ex: Throwable) =
          observer.onError(ex)

        def onCompleted() =
          observer.onCompleted()
      }))

  /**
   * Returns an Observable which only emits those items for which a given predicate holds.
   *
   * @param p a function that evaluates the items emitted by the source Observable, returning `true` if they pass the filter
   * @return an Observable that emits only those items in the original Observable for which the filter evaluates as `true`
   */
  def filter(p: A => Boolean): Observable[A] =
    Observable.create(observer => subscribe(new Observer[A] {
      def onNext(elem: A) = {
        // See Section 6.4. - Protect calls to user code from within an operator - in the Rx Design Guidelines
        // Note: onNext must not be protected, as it's on the edge of the monad and protecting it yields weird effects
        var streamError = true
        try {
          val r = p(elem)
          streamError = false
          if (r)
            observer.onNext(elem)
          else
            Continue
        }
        catch {
          case NonFatal(ex) if streamError =>
            observer.onError(ex)
            Stop
        }
      }
      def onError(ex: Throwable) = observer.onError(ex)
      def onCompleted() = observer.onCompleted()
    }))

  /**
   * Returns an Observable which only emits the first item for which the predicate holds.
   *
   * @param p a function that evaluates the items emitted by the source Observable, returning `true` if they pass the filter
   * @return an Observable that emits only the first item in the original Observable for which the filter evaluates as `true`
   */
  final def find(p: A => Boolean): Observable[A] =
    filter(p).head

  /**
   * Returns an Observable which emits a single value, either true, in case the given predicate holds for at least
   * one item, or false otherwise.
   *
   * @param p a function that evaluates the items emitted by the source Observable, returning `true` if they pass the filter
   * @return an Observable that emits only true or false in case the given predicate holds or not for at least one item
   */
  final def exists(p: A => Boolean): Observable[Boolean] =
    find(p).foldLeft(false)((_, _) => true)

  /**
   * Returns an Observable that emits a single boolean, either true, in case the given predicate holds for all the items
   * emitted by the source, or false in case at least one item is not verifying the given predicate.
   *
   * @param p a function that evaluates the items emitted by the source Observable, returning `true` if they pass the filter
   * @return an Observable that emits only true or false in case the given predicate holds or not for all the items
   */
  final def forAll(p: A => Boolean): Observable[Boolean] =
    exists(e => !p(e)).map(r => !r)

  /**
   * Creates a new Observable by applying a function that you supply to each item emitted by
   * the source Observable, where that function returns an Observable, and then merging those
   * resulting Observables and emitting the results of this merger.
   *
   * @param f a function that, when applied to an item emitted by the source Observable, returns an Observable
   * @return an Observable that emits the result of applying the transformation function to each
   *         item emitted by the source Observable and merging the results of the Observables
   *         obtained from this transformation.
   */
  def flatMap[B](f: A => Observable[B]): Observable[B] =
    Observable.create(observer => {
      // we need to do ref-counting for triggering `onCompleted` on our subscriber
      // when all the children threads have ended
      val refCounter = RefCountCancelable(observer.onCompleted())
      val composite = CompositeCancelable()

      composite += subscribe(new Observer[A] {
        def onNext(elem: A) = {
          // reference that gets released when the child observer is completed
          val refID = refCounter.acquireCancelable()
          // cancelable reference created for child threads spawned by this flatMap
          // ... is different than `refID` as it serves the purpose of cancelling
          // everything on `cancel()`
          val sub = SingleAssignmentCancelable()
          composite += sub

          val childObserver = new Observer[B] {
            def onNext(elem: B) =
              observer.onNext(elem)

            def onError(ex: Throwable) =
              // onError, cancel everything
              try observer.onError(ex) finally composite.cancel()

            def onCompleted() = {
              // do resource release, otherwise we can end up with a memory leak
              composite -= sub
              refID.cancel()
              sub.cancel()
            }
          }

          // See Section 6.4. - Protect calls to user code from within an operator - in the Rx Design Guidelines
          // Note: onNext must not be protected, as it's on the edge of the monad and protecting it yields weird effects
          var streamError = true
          try {
            val childObs = f(elem)
            streamError = false
            sub := childObs.subscribe(childObserver)
            Continue
          }
          catch {
            case NonFatal(ex) if streamError =>
              observer.onError(ex)
              Stop
          }
        }

        def onError(ex: Throwable) =
          try observer.onError(ex) finally composite.cancel()

        def onCompleted() = {
          // triggers observer.onCompleted() when all Observables created have been finished
          // basically when the main thread is completed, it waits to stream onCompleted
          // until all children have been onCompleted too - only after that `subscriber.onCompleted` gets triggered
          // (see `RefCountCancelable` for details on how it works)
          refCounter.cancel()
        }
      })

      composite
    })

  /**
   * Flattens the sequence of Observables emitted by `this` into one Observable, without any
   * transformation.
   *
   * You can combine the items emitted by multiple Observables so that they act like a single
   * Observable by using this method.
   *
   * This operation is only available if `this` is of type `Observable[Observable[B]]` for some `B`,
   * otherwise you'll get a compilation error.
   *
   * @return an Observable that emits items that are the result of flattening the items emitted
   *         by the Observables emitted by `this`
   */
  final def flatten[B](implicit ev: A <:< Observable[B]): Observable[B] =
    flatMap(x => x)

  /**
   * Flattens two Observables into one Observable, without any transformation.
   *
   * @param other an Observable to be merged
   * @return an Observable that emits items from `this` and `that` until
   *         `this` or `that` emits `onError` or `onComplete`.   */
  final def merge[B >: A](other: Observable[B]): Observable[B] =
    Observable.fromTraversable(Seq(this, other)).flatMap(x => x)

  final def head: Observable[A] = take(1)

  final def tail: Observable[A] = drop(1)

  final def headOrElse[B >: A](default: => B): Observable[B] =
    head.foldLeft(Option.empty[A])((_, elem) => Some(elem)).map {
      case Some(elem) => elem
      case None => default
    }

  final def firstOrElse[B >: A](default: => B): Observable[B] =
    headOrElse(default)

  final def take(nr: Int): Observable[A] = {
    require(nr > 0, "number of elements to take should be strictly positive")

    Observable.create(observer => subscribe(new Observer[A] {
      val count = Atomic(0)

      @tailrec
      def onNext(elem: A): Ack = {
        val currentCount = count.get

        if (currentCount < nr) {
          val newCount = currentCount + 1
          if (!count.compareAndSet(currentCount, newCount))
            onNext(elem)
          else {
            observer.onNext(elem)
            if (newCount == nr) {
              observer.onCompleted()
              Stop
            }
            else
              Continue
          }
        }
        else
          Stop
      }

      def onCompleted(): Unit =
        observer.onCompleted()

      def onError(ex: Throwable): Unit =
        observer.onError(ex)
    }))
  }

  final def drop(nr: Int): Observable[A] = {
    require(nr > 0, "number of elements to drop should be strictly positive")

    Observable.create(observer => subscribe(new Observer[A] {
      val count = Atomic(0)

      @tailrec
      def onNext(elem: A): Ack = {
        val currentCount = count.get

        if (currentCount < nr) {
          val newCount = currentCount + 1
          if (!count.compareAndSet(currentCount, newCount))
            onNext(elem)
          else
            Continue
        }
        else
          observer.onNext(elem)
      }

      def onCompleted(): Unit =
        observer.onCompleted()

      def onError(ex: Throwable): Unit =
        observer.onError(ex)
    }))
  }

  final def takeWhile(p: A => Boolean): Observable[A] =
    Observable.create(observer => subscribe(new Observer[A] {
      val shouldContinue = Atomic(true)

      def onNext(elem: A): Ack = {
        var streamError = true
        try {
          if (shouldContinue.get) {
            val update = p(elem)
            streamError = false

            if (shouldContinue.compareAndSet(expect=true, update=update) && update) {
              observer.onNext(elem)
              Continue
            }
            else if (!update) {
              observer.onCompleted()
              Stop
            }
            else
              Stop
          }
          else
            Stop
        }
        catch {
          case NonFatal(ex) if streamError =>
            observer.onError(ex)
            Stop
        }
      }

      def onCompleted(): Unit =
        observer.onCompleted()

      def onError(ex: Throwable): Unit =
        observer.onError(ex)
    }))

  final def dropWhile(p: A => Boolean): Observable[A] =
    Observable.create(observer => subscribe(new Observer[A] {
      val shouldDropRef = Atomic(true)

      @tailrec
      def onNext(elem: A): Ack =
        if (!shouldDropRef.get)
          observer.onNext(elem)
        else {
          val shouldDrop = p(elem)
          if (!shouldDropRef.compareAndSet(expect=true, update=shouldDrop) || !shouldDrop)
            onNext(elem)
          else
            Continue
        }

      def onCompleted(): Unit =
        observer.onCompleted()

      def onError(ex: Throwable): Unit =
        observer.onError(ex)
    }))

  final def foldLeft[R](initial: R)(f: (R, A) => R): Observable[R] =
    Observable.create { observer =>
      val state = Atomic(initial)

      subscribe(new Observer[A] {
        def onNext(elem: A): Ack =
          try {
            state.transformAndGet(s => f(s, elem))
            Continue
          }
          catch {
            case NonFatal(ex) =>
              observer.onError(ex)
              Stop
          }

        def onCompleted(): Unit = {
          observer.onNext(state.get)
          observer.onCompleted()
        }

        def onError(ex: Throwable): Unit =
          observer.onError(ex)
      })
    }

  final def ++[B >: A](other: => Observable[B]): Observable[B] =
    Observable.create[B](observer => subscribe(
      SynchronizedObserver(new Observer[A] {
        def onNext(elem: A) = observer.onNext(elem)
        def onError(ex: Throwable) = observer.onError(ex)
        def onCompleted() = other.subscribe(observer)
      })))

  /**
   * Executes the given callback when the stream has ended on `onCompleted`
   *
   * NOTE: make sure that the specified callback doesn't throw errors, because
   * it gets executed when `cancel()` happens and by definition the error cannot
   * be streamed with `onError()` and so the behavior is left as undefined, possibly
   * crashing the application or worse - leading to non-deterministic behavior.
   *
   * @param cb the callback to execute when the subscription is canceled
   */
  final def doOnCompleted(cb: => Unit): Observable[A] =
    Observable.create { observer =>
      subscribe(new Observer[A] {
        def onNext(elem: A) =
          observer.onNext(elem)

        def onError(ex: Throwable) =
          observer.onError(ex)

        def onCompleted() = {
          observer.onCompleted()
          cb
        }
      })
    }

  /**
   * Executes the given callback for each element generated by the source
   * Observable, useful for doing side-effects.
   *
   * @return a new Observable that executes the specified callback for each element
   */
  final def doWork(cb: A => Unit): Observable[A] =
    Observable.create(observer => subscribe(new Observer[A] {
      def onNext(elem: A) = {
        // See Section 6.4. - Protect calls to user code from within an operator - in the Rx Design Guidelines
        // Note: onNext must not be protected, as it's on the edge of the monad and protecting it yields weird effects
        var streamError = true
        try {
          cb(elem)
          streamError = false
          observer.onNext(elem)
        }
        catch {
          case NonFatal(ex) if streamError =>
            observer.onError(ex)
            Stop
        }
      }

      def onError(ex: Throwable): Unit =
        observer.onError(ex)

      def onCompleted(): Unit =
        observer.onCompleted()
    }))

  final def zip[B](other: Observable[B]): Observable[(A,B)] =
    Observable.create { observer =>
      val composite = CompositeCancelable()
      val lock = NaiveSpinLock()
      val queueA = mutable.Queue.empty[A]
      val queueB = mutable.Queue.empty[B]
      var aIsDone = false
      var bIsDone = false

      def _onError(ex: Throwable) =
        lock.acquire {
          aIsDone = true
          bIsDone = true
          queueA.clear()
          queueB.clear()
          observer.onError(ex)
        }

      composite += subscribe(new Observer[A] {
        def onNext(elem: A) =
          lock.acquire {
            if (!aIsDone)
              if (queueB.nonEmpty) {
                val b = queueB.dequeue()
                observer.onNext((elem, b))
              }
              else if (bIsDone) {
                onCompleted()
                Stop
              }
              else {
                queueA.enqueue(elem)
                Continue
              }
            else
              Stop
          }

        def onCompleted(): Unit =
          lock.acquire {
            if (!aIsDone) {
              aIsDone = true
              if (queueA.isEmpty || bIsDone) {
                queueA.clear()
                observer.onCompleted()
              }
            }
          }

        def onError(ex: Throwable): Unit =
          _onError(ex)
      })

      composite += other.subscribe(new Observer[B] {
        def onNext(elem: B) =
          lock.acquire {
            if (!bIsDone)
              if (queueA.nonEmpty) {
                val a = queueA.dequeue()
                observer.onNext((a, elem))
              }
              else if (aIsDone) {
                onCompleted()
                Stop
              }
              else {
                queueB.enqueue(elem)
                Continue
              }
            else
              Stop
          }

        def onCompleted(): Unit =
          lock.acquire {
            if (!bIsDone) {
              bIsDone = true
              if (queueB.isEmpty || aIsDone) {
                queueB.clear()
                observer.onCompleted()
              }
            }
          }

        def onError(ex: Throwable): Unit =
          _onError(ex)
      })

      composite
    }

  /**
   * Returns the first generated result as a Future and then cancels
   * the subscription.
   */
  final def asFuture(implicit ec: ExecutionContext): Future[Option[A]] = {
    val promise = Promise[Option[A]]()

    head.subscribe(new Observer[A] {
      def onNext(elem: A): Ack = {
        promise.trySuccess(Some(elem))
        Stop
      }

      def onError(ex: Throwable): Unit =
        promise.tryFailure(ex)


      def onCompleted(): Unit =
        promise.trySuccess(None)
    })

    promise.future
  }

  final def safe: Observable[A] =
    Observable.create(observer => subscribe(SynchronizedObserver(observer)))
}

object Observable {
  def create[A](f: Observer[A] => Cancelable): Observable[A] =
    new Observable[A] {
      def subscribe(observer: Observer[A]) =
        try f(observer) catch {
          case NonFatal(ex) =>
            observer.onError(ex)
            Cancelable.alreadyCanceled
        }
    }

  def empty[A]: Observable[A] =
    create { observer =>
      observer.onCompleted()
      Cancelable.alreadyCanceled
    }

  def unit[A](elem: A): Observable[A] =
    create { observer =>
      observer.onNext(elem)
      observer.onCompleted()
      Cancelable.alreadyCanceled
    }

  def error(ex: Throwable): Observable[Nothing] =
    create { observer =>
      observer.onError(ex)
      Cancelable.alreadyCanceled
    }

  def never: Observable[Nothing] =
    create { _ => Cancelable() }

  def fromTraversable[T](sequence: TraversableOnce[T]): Observable[T] =
    create[T] { observer =>
      var alreadyStopped = false

      Try(sequence.toIterator) match {
        case Success(iterator) =>
          var shouldContinue = true

          while (shouldContinue) {
            var streamError = true
            try {
              if (iterator.hasNext) {
                val next = iterator.next()
                streamError = false
                alreadyStopped = observer.onNext(next) == Stop
                shouldContinue = !alreadyStopped
              }
              else
                shouldContinue = false
            }
            catch {
              case NonFatal(ex) if streamError =>
                observer.onError(ex)
                shouldContinue = false
            }
          }

        case Failure(ex) =>
          observer.onError(ex)
      }

      if (!alreadyStopped) observer.onCompleted()
      Cancelable.alreadyCanceled
    }

  def merge[T](sources: Observable[T]*): Observable[T] =
    Observable.fromTraversable(sources).flatten
}