/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.pipelined.state

import java.util.concurrent.atomic.AtomicBoolean

import org.neo4j.cypher.internal.runtime.debug.DebugSupport

/**
 * Basic lock.
 */
trait Lock {
  /**
   * Try to acquire this lock.
   * @return true iff the lock was acquired
   */
  def tryLock(): Boolean

  /**
   * Release this lock.
   */
  def unlock(): Unit
}

/**
 * Dummy implementation of [[Lock]] which can be used to catch
 * trivial bugs in single threaded execution.
 *
 * @param id some id that can be used to identify this lock instance
 */
class NoLock(val id: String) extends Lock {

  private var isLocked = false

  override def tryLock(): Boolean = {
    if (isLocked)
      throw new IllegalStateException(s"$name is already locked")
    isLocked = true
    if (DebugSupport.LOCKS.enabled) {
      DebugSupport.LOCKS.log("Locked %s", name)
    }
    isLocked
  }

  override def unlock(): Unit = {
    if (!isLocked)
      throw new IllegalStateException(s"$name is not locked")
    if (DebugSupport.LOCKS.enabled) {
      DebugSupport.LOCKS.log("Unlocked %s", name)
    }
    isLocked = false
  }

  private def name: String = s"${getClass.getSimpleName}[$id]"
  override def toString: String = s"$name: Locked=$isLocked"
}

/**
 * Proper concurrent implementation of [[Lock]] based on [[AtomicBoolean]].
 *
 * @param id some id that can be used to identify this lock instance
 */
class ConcurrentLock(val id: String) extends Lock {
  private val isLocked = new AtomicBoolean(false)

  override def tryLock(): Boolean = {
    val success = isLocked.compareAndSet(false, true)
    if (DebugSupport.LOCKS.enabled) {
      DebugSupport.LOCKS.log("%s tried locking %s. Success: %s", Thread.currentThread().getId, name, success)
    }
    success
  }

  override def unlock(): Unit = {
    if (!isLocked.compareAndSet(true, false)) {
      throw new IllegalStateException(s"${Thread.currentThread().getId} tried to release $name that was not acquired.")
    } else {
      if (DebugSupport.LOCKS.enabled) {
        DebugSupport.LOCKS.log("%s unlocked %s.", Thread.currentThread().getId, name)
      }
    }
  }

  private def name: String = s"${getClass.getSimpleName}[$id]"
  override def toString: String = s"$name: Locked=${isLocked.get()}"
}
