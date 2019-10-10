package org.neo4j.cypher.internal.runtime.morsel

import scala.util.control.{ControlThrowable, NonFatal}

/**
 * Extractor of non-fatal Throwables.
 *
 * For the most part this class delegates to [[scala.util.control.NonFatal]],
 * which not match fatal errors like [[VirtualMachineError]] (e.g., [[OutOfMemoryError]]).
 * See [[scala.util.control.NonFatal]] for more info.
 */
object NonFatalToRuntime {
  /**
   * Returns true if the provided `Throwable` is to be considered non-fatal, or false if it is to be considered fatal
   */
  def apply(t: Throwable): Boolean = t match {
    // NonFatal considers StackOverflowError as fatal, but if we catch it we can at least inform the user via QuerySubscriber.onError()
    case _:StackOverflowError => true
    case NonFatal(_) => false
    case _ => true
  }
  /**
   * Returns Some(t) if NonFatalToRuntime(t) == true, otherwise None
   */
  def unapply(t: Throwable): Option[Throwable] = Some(t).filter(apply)
}
