package kafka.delay.test.unit.utils

import java.util.concurrent.{Future, TimeUnit}

object JavaFutures {
  class JavaSuccess[T](result: T) extends Future[T] {
    override def cancel(mayInterruptIfRunning: Boolean): Boolean = false

    override def isCancelled: Boolean = false

    override def isDone: Boolean = true

    override def get(): T = result

    override def get(timeout: Long, unit: TimeUnit): T = result
  }

  class JavaFailure[T](reason: Throwable) extends Future[T] {
    override def cancel(mayInterruptIfRunning: Boolean): Boolean = false

    override def isCancelled: Boolean = false

    override def isDone: Boolean = true

    override def get(): T = throw reason

    override def get(timeout: Long, unit: TimeUnit): T = throw reason
  }

  def success[T](result: T): Future[T] = new JavaSuccess(result)
  def failed[T](reason: Throwable): Future[T] = new JavaFailure(reason)
}
