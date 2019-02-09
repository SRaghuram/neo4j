/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.zombie.state

import java.util.concurrent.atomic.AtomicBoolean

import org.neo4j.cypher.internal.runtime.zombie.Zombie

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
class NoLock(val id: Int) extends Lock {

  private var isLocked = false

  override def tryLock(): Boolean = {
    if (isLocked)
      throw new IllegalStateException("NoLock is already locked")
    Zombie.debug(s"Locked #$id")
    isLocked = true
    isLocked
  }

  override def lock(): Unit = tryLock()

  override def unlock(): Unit = {
    if (!isLocked)
      throw new IllegalStateException("NoLock is not locked")
    Zombie.debug(s"Unlocked #$id")
    isLocked = false
  }
}

/**
  * Proper concurrent implementation of [[Lock]] based on [[AtomicBoolean]].
  *
  * @param id some id that can be used to identify this lock instance
  */
class ConcurrentLock(val id: Int) extends Lock {
  private val isLocked = new AtomicBoolean(false)

  override def tryLock(): Boolean =
    isLocked.compareAndSet(false, true)

  override def lock(): Unit =
    while (!isLocked.compareAndSet(false, true)) {}

  override def unlock(): Unit = {
    if (!isLocked.compareAndSet(true, false)) {
      throw new IllegalStateException("Tried to release a lock that was not acquired")
    }
  }

  override def toString: String = s"${getClass.getSimpleName}[$id]: Locked=${isLocked.get()}"
}
