/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.morsel.state

import java.util.concurrent.atomic.AtomicBoolean

import org.neo4j.cypher.internal.runtime.morsel.debug

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
    * Acquire this lock. Will retry until successful.
    */
  def lock(): Unit

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
    isLocked
  }

  override def lock(): Unit = tryLock()

  override def unlock(): Unit = {
    if (!isLocked)
      throw new IllegalStateException(s"$name is not locked")
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
    debug(s"${Thread.currentThread().getId} tried locking $name. Success: $success")
    success
  }

  override def lock(): Unit =
    while (!isLocked.compareAndSet(false, true)) {}

  override def unlock(): Unit = {
    if (!isLocked.compareAndSet(true, false)) {
      throw new IllegalStateException(s"${Thread.currentThread().getId} tried to release $name that was not acquired.")
    } else {
      debug(s"${Thread.currentThread().getId} unlocked $name.")
    }
  }

  private def name: String = s"${getClass.getSimpleName}[$id]"
  override def toString: String = s"$name: Locked=${isLocked.get()}"
}
